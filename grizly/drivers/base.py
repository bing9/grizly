from abc import ABC, abstractmethod
from copy import deepcopy
import decimal
from functools import partial
import logging
import re
from typing import Any, Callable, List, Optional, Tuple, TypeVar, Union

import deprecation
from pandas import DataFrame
import pyarrow as pa

from ..store import Store
from ..utils.type_mappers import (
    python_to_sql,
    rds_to_pyarrow,
    sql_to_python,
)
from ..utils.functions import dict_diff

deprecation.deprecated = partial(deprecation.deprecated, deprecated_in="0.4", removed_in="0.5")


class BaseDriver(ABC):
    _allowed_agg = ["SUM", "COUNT", "MAX", "MIN", "AVG", "STDDEV", ""]
    _allowed_group_by = _allowed_agg + ["GROUP"]
    _allowed_order_by = ["ASC", "DESC", ""]

    def __init__(
        self,
        store: Optional[Store] = None,
        json_path: str = None,
        subquery: str = None,
        logger: logging.Logger = None,
        *args,
        **kwargs,
    ):
        self.logger = logger or logging.getLogger(__name__)
        self.getfields = kwargs.get("getfields")

        data = kwargs.get("data")
        if data:
            store = data
            self.logger.warning(
                "Parameter data in QFrame is deprecated as of 0.4 and will be removed in 0.4.5."
                " Please use store parameter instead.",
            )

        self.store = self._load_store(store=store, json_path=json_path, subquery=subquery,)

    def _load_store(
        self, store: Store = None, json_path: str = None, subquery: str = None,
    ) -> Store:
        if store:
            store = Store(store)
        elif json_path:
            store = Store().from_json(json_path=json_path, subquery=subquery)
        else:
            store = Store()

        if store != Store():
            store = self.validate_data(store)
        return store

    def __len__(self) -> int:
        return self.nrows

    # TODO: todl
    def __getitem__(self, getfields):
        if isinstance(getfields, str):
            self.getfields = [getfields]
        elif isinstance(getfields, tuple):
            self.getfields = list(getfields)
        else:
            self.getfields = getfields
        return self

    @abstractmethod
    def to_records(self) -> List[Tuple[Any]]:
        pass

    @property
    def nrows(self) -> int:
        records = self.to_records()
        return len(records)

    @property
    def data(self):
        return self.store

    @property
    def ncols(self) -> int:
        ncols = len(self.get_fields())
        return ncols

    @property
    def shape(self):
        nrows = self.nrows
        ncols = self.ncols
        shape = (nrows, ncols)
        return shape

    @property
    def columns(self):
        return self.get_fields()

    @property
    def fields(self):
        """Alias for QFrame.columns"""
        return self.columns

    @property
    def dtypes(self):
        return self.get_dtypes()

    @property
    def types(self):
        """Alias for QFrame.dtypes"""
        return self.dtypes

    def select(self, fields: List[str]):
        """TO Review: if select is dict create fields
        maybe this is not good workflow though might
        be confusing

        Parameters
        ----------
        fields : list
            List of fields to select

        Returns
        -------
        QFrame
        """
        fields = self._get_fields_names(fields)

        for field in self.get_fields():
            if field not in fields:
                self.store["select"]["fields"].pop(field, None)
        return self

    def rename(self, fields: dict):
        """Renames columns (changes the field alias).

        Parameters
        ----------
        fields : dict
            Dictionary of columns and their new names.

        Examples
        --------
        >>> qf = QFrame(dsn="redshift_acoe", schema="grizly", table="sales")
        >>> qf = qf.rename({'sales': 'Billings'})
        >>> print(qf)
        SELECT "customer_id",
               "sales" AS "Billings"
        FROM grizly.sales

        Returns
        -------
        QFrame
        """
        if not isinstance(fields, dict):
            raise ValueError("Fields parameter should be of type dict.")

        fields_names, not_found_fields = self._get_fields_names(fields, not_found=True)

        for field in not_found_fields:
            fields.pop(field)

        for field, field_nm in zip(fields.keys(), fields_names):
            self.store["select"]["fields"][field_nm]["as"] = fields[field]
        return self

    def remove(self, fields: list):
        """Removes fields.

        Parameters
        ----------
        fields : list
            List of fields to remove.

        Examples
        --------
        >>> qf = QFrame(dsn="redshift_acoe", schema="grizly", table="sales")
        >>> qf = qf.remove(['sales'])
        >>> print(qf)
        SELECT "customer_id"
        FROM grizly.sales

        Returns
        -------
        QFrame
        """
        if isinstance(fields, str):
            fields = [fields]

        fields = self._get_fields_names(fields)

        for field in fields:
            self.store["select"]["fields"].pop(field, f"Field {field} not found.")

        return self

    def distinct(self):
        """Adds DISTINCT statement.

        Examples
        --------
        >>> qf = QFrame(dsn="redshift_acoe", schema="grizly", table="sales")
        >>> qf = qf.distinct()
        >>> print(qf)
        SELECT DISTINCT "customer_id",
                        "sales"
        FROM grizly.sales

        Returns
        -------
        QFrame
        """
        self.store["select"]["distinct"] = 1

        return self

    @deprecation.deprecated(details="Use QFrame.where instead",)
    def query(self, query: str, if_exists: str = "append", operator: str = "and"):
        return self.where(query=query, if_exists=if_exists, operator=operator)

    def where(self, query: str, if_exists: str = "append", operator: str = "and"):
        """Adds WHERE statement.

        Parameters
        ----------
        query : str
            Where statement.
        if_exists : {'append', 'replace'}, optional
            How to behave when the where clause already exists, by default 'append'
        operator : {'and', 'or'}, optional
            How to add another condition to existing one, by default 'and'

        Examples
        --------
        >>> qf = QFrame(dsn="redshift_acoe", schema="grizly", table="sales")
        >>> qf = qf.query("sales != 0")
        >>> print(qf)
        SELECT "customer_id",
               "sales"
        FROM grizly.sales
        WHERE sales != 0

        Returns
        -------
        QFrame
        """
        if if_exists not in ["append", "replace"]:
            raise ValueError("Invalid value in if_exists. Valid values: 'append', 'replace'.")
        if operator not in ["and", "or"]:
            raise ValueError("Invalid value in operator. Valid values: 'and', 'or'.")

        # TODO: this should be a decorator in SQLDriver
        # if "union" in self.store["select"]:
        #     self.logger.info("You can't add where clause inside union. Use select() method first.")
        # else:
        if (
            "where" not in self.store["select"]
            or self.store["select"]["where"] == ""
            or if_exists == "replace"
        ):
            self.store["select"]["where"] = query
        elif if_exists == "append":
            self.store["select"]["where"] += f" {operator} {query}"
        return self

    def having(self, having: str, if_exists: str = "append", operator: str = "and"):
        """Adds HAVING statement.

        Parameters
        ----------
        having : str
            Having statement.
        if_exists : {'append', 'replace'}, optional
            How to behave when the having clause already exists, by default 'append'
        operator : {'and', 'or'}, optional
            How to add another condition to existing one, by default 'and'

        Examples
        --------
        >>> qf = QFrame(dsn="redshift_acoe", schema="grizly", table="sales")
        >>> qf = qf.groupby(['customer_id'])['sales'].agg('sum')
        >>> qf = qf.having("sum(sales)>100")
        >>> print(qf)
        SELECT "customer_id",
               sum("sales") AS "sales"
        FROM grizly.sales
        GROUP BY 1
        HAVING sum(sales)>100

        Returns
        -------
        QFrame
        """
        if if_exists not in ["append", "replace"]:
            raise ValueError("Invalid value in if_exists. Valid values: 'append', 'replace'.")
        if operator not in ["and", "or"]:
            raise ValueError("Invalid value in operator. Valid values: 'and', 'or'.")

        # if "union" in self.store["select"]:
        #     self.logger.info(
        #         """You can't add having clause inside union. Use select() method first.
        #     (The GROUP BY and HAVING clauses are applied to each individual query, not the final result set.)"""
        #     )

        # else:
        if (
            "having" not in self.store["select"]
            or self.store["select"]["having"] == ""
            or if_exists == "replace"
        ):
            self.store["select"]["having"] = having
        elif if_exists == "append":
            self.store["select"]["having"] += f" {operator} {having}"
        return self

    def assign(
        self, group_by: str = "", order_by: str = "", dtype: str = "VARCHAR(500)", **kwargs,
    ):
        """Assigns expressions.

        Parameters
        ----------
        group_by : {group, sum, count, min, max, avg, stddev ""}, optional
            Aggregation type, by default ""
        order_by : {'ASC','DESC'}, optional
            Sort ascending or descending, by default ''
        dtype : str, optional
            Column type, by default 'VARCHAR(500)'

        Examples
        --------
        >>> qf = QFrame(dsn="redshift_acoe", schema="grizly", table="sales")
        >>> qf = qf.assign(sales_Div="sales/100", dtype='float')
        >>> print(qf)
        SELECT "customer_id",
               "sales",
               sales/100 AS "sales_Div"
        FROM grizly.sales

        >>> qf = QFrame(dsn="redshift_acoe", schema="grizly", table="sales")
        >>> qf = qf.assign(sales_Positive="CASE WHEN sales>0 THEN 1 ELSE 0 END", dtype='float')
        >>> print(qf)
        SELECT "customer_id",
               "sales",
               CASE
                   WHEN sales>0 THEN 1
                   ELSE 0
               END AS "sales_Positive"
        FROM grizly.sales

        Returns
        -------
        QFrame
        """
        custom_type = kwargs.get("custom_type")
        _type = kwargs.get("type")
        if custom_type:
            dtype = custom_type
            self.logger.warning(
                "Parameter 'custom_type' in method QFrame.assign"
                " is deprecated as of 0.4 and will be removed"
                " in 0.4.5. Use 'dtype' instead."
            )
        elif _type:
            self.logger.warning(
                "Parameter 'type' in method QFrame.assign"
                " is deprecated as of 0.4 and will be removed"
                " in 0.4.5. Use 'dtype' instead."
            )
            if _type == "num":
                dtype = "FLOAT(53)"

        if group_by.upper() not in self._allowed_group_by:
            raise ValueError(f"Invalid value in group_by. Valid values: {self._allowed_group_by}.")
        if order_by.upper() not in self._allowed_order_by:
            raise ValueError(f"Invalid value in order_by. Valid values: {self._allowed_order_by}.")
        # if "union" in self.store["select"]:
        #     self.logger.warning(
        #         "You can't assign expressions inside union. Use select() method first."
        #     )
        # else:
        if kwargs is not None:
            for key, expression in kwargs.items():
                if key in ["custom_type", "type"]:
                    continue
                self.store["select"]["fields"][key] = {
                    "dtype": dtype,
                    "as": key,
                    "group_by": group_by,
                    "order_by": order_by,
                    "expression": expression,
                }
        return self

    def groupby(self, fields: Union[List[str], str] = None):
        """Adds GROUP BY statement.

        Parameters
        ----------
        fields : list or string
            List of fields or a field, if None then all fields are grouped

        Examples
        --------
        >>> qf = QFrame(dsn="redshift_acoe", schema="grizly", table="sales")
        >>> qf = qf.groupby(['customer_id'])['sales'].agg('sum')
        >>> print(qf)
        SELECT "customer_id",
               sum("sales") AS "sales"
        FROM grizly.sales
        GROUP BY 1

        Returns
        -------
        QFrame
        """
        # assert (
        #     "union" not in self.store["select"]
        # ), "You can't group by inside union. Use select() method first."

        if isinstance(fields, str):
            fields = [fields]

        if fields is None:
            fields = self.get_fields(not_selected=True)
        else:
            fields = self._get_fields_names(fields)

        for field in fields:
            self.store["select"]["fields"][field]["group_by"] = "group"

        return self

    def agg(self, aggtype):
        """Aggregates fields.

        Parameters
        ----------
        aggtype : {'sum', 'count', 'min', 'max', 'avg', 'stddev'}
            Aggregation type.

        Examples
        --------
        >>> qf = QFrame(dsn="redshift_acoe", schema="grizly", table="table_tutorial")
        >>> qf = qf.groupby(['col1', 'col2'])['col3', 'col4'].agg('sum')
        >>> print(qf)
        SELECT "col1",
               "col2",
               sum("col3") AS "col3",
               sum("col4") AS "col4"
        FROM grizly.table_tutorial
        GROUP BY 1,
                 2

        Returns
        -------
        QFrame
        """
        if aggtype.upper() not in self._allowed_agg:
            raise ValueError(f"Invalid value in aggtype. Valid values: {self._allowed_agg}.")

        # if "union" in self.store["select"]:
        #     self.logger.warning("You can't aggregate inside union. Use select() method first.")
        # else:
        self.getfields = self._get_fields_names(self.getfields, aliased=False)
        for field in self.getfields:
            self.store["select"]["fields"][field]["group_by"] = aggtype

        return self

    def sum(self):
        """Sums fields that have nothing in group_by key.

        Examples
        --------
        >>> columns=["col1", "col2", "col3"]
        >>> qf = QFrame(dsn="redshift_acoe", schema="grizly", table="table_tutorial", columns=columns)
        >>> qf = qf.groupby(['col1']).sum()
        >>> print(qf)
        SELECT "col1",
               sum("col2") AS "col2",
               sum("col3") AS "col3"
        FROM grizly.table_tutorial
        GROUP BY 1

        Returns
        -------
        QFrame
        """
        fields = []
        for field in self.store["select"]["fields"]:
            if self.store["select"]["fields"][field].get("group_by", "") == "":
                fields.append(field)
        return self[fields].agg("sum")

    def orderby(self, fields: list, ascending: Union[bool, List[bool]] = True):
        """Adds ORDER BY statement.

        Parameters
        ----------
        fields : list or str
            Fields in list or field as a string.
        ascending : bool or list, optional
            Sort ascending vs. descending. Specify list for multiple sort orders, by default True

        Examples
        --------
        >>> qf = QFrame(dsn="redshift_acoe", schema="grizly", table="sales")
        >>> qf = qf.orderby(["sales"])
        >>> print(qf)
        SELECT "customer_id",
               "sales"
        FROM grizly.sales
        ORDER BY 2

        >>> qf = QFrame(dsn="redshift_acoe", schema="grizly", table="sales")
        >>> qf = qf.orderby(["sales"], ascending=False)
        >>> print(qf)
        SELECT "customer_id",
               "sales"
        FROM grizly.sales
        ORDER BY 2 DESC

        Returns
        -------
        QFrame
        """
        if isinstance(fields, str):
            fields = [fields]
        if isinstance(ascending, bool):
            ascending = [ascending for _ in fields]

        assert len(fields) == len(ascending), "Incorrect list size."

        fields = self._get_fields_names(fields)

        iterator = 0
        for field in fields:
            if field in self.store["select"]["fields"]:
                order = "ASC" if ascending[iterator] else "DESC"
                self.store["select"]["fields"][field]["order_by"] = order
            else:
                self.logger.warning(f"Field {field} not found.")

            iterator += 1

        return self

    def limit(self, limit: int):
        """Adds LIMIT statement.

        Parameters
        ----------
        limit : int or str
            Number of rows to select.

        Examples
        --------
        >>> qf = QFrame(dsn="redshift_acoe", schema="grizly", table="sales")
        >>> qf = qf.limit(100)
        >>> print(qf)
        SELECT "customer_id",
               "sales"
        FROM grizly.sales
        LIMIT 100

        Returns
        -------
        QFrame
        """
        self.store["select"]["limit"] = str(limit)

        return self

    def offset(self, offset: int):
        """Adds OFFSET statement.

        Parameters
        ----------
        offset : int or str
            The row from which to start the data.

        Examples
        --------
        >>> qf = QFrame(dsn="redshift_acoe", schema="grizly", table="sales")
        >>> qf = qf.offset(100)
        >>> print(qf)
        SELECT "customer_id",
               "sales"
        FROM grizly.sales
        OFFSET 100

        Returns
        -------
        QFrame
        """
        self.store["select"]["offset"] = str(offset)

        return self

    def window(
        self,
        offset: int = None,
        limit: int = None,
        deterministic: bool = True,
        order_by: list = None,
    ):
        """Sorts records and adds LIMIT and OFFSET parameters to QFrame, creating a chunk.

        Parameters
        ----------
        offset : int, optional
            The row from which to start the data, by default None
        limit : int, optional
            Number of rows to select, by default None
        deterministic : bool, optional
            Whether the result should be deterministic, by default True
        order_by : list or str, optional
            List of fields that should be used to sort data. If None than data is sorted by all fields, by default None

        Examples
        --------
        >>> qf = QFrame(dsn="redshift_acoe", schema="grizly", table="sales")
        >>> qf = qf.window(5, 10)
        >>> print(qf)
        SELECT "customer_id",
               "sales"
        FROM grizly.sales
        ORDER BY 1,
                 2
        OFFSET 5
        LIMIT 10

        Returns
        -------
        QFrame
        """
        if deterministic:
            if order_by is not None:

                if not self.__check_if_values_are_distinct(columns=order_by):
                    raise ValueError(
                        "Selected columns don't give distinct records. Please change 'order_by' parameter or remove it."
                    )

                self.orderby(order_by)

            else:
                self.orderby(self.get_fields())

        if offset is not None:
            self.offset(offset)

        if limit is not None:
            self.limit(limit)

        return self

    def __check_if_values_are_distinct(self, columns):
        qf1 = self.copy()
        qf2 = self.copy()
        qf2.select(columns)
        if len(qf1.distinct()) != len(qf2.distinct()):
            return False
        return True

    def cut(self, chunksize: int, deterministic: bool = True, order_by: list = None):
        """Divides a QFrame into multiple smaller QFrames, each containing chunksize rows.

        Parameters
        ----------
        chunksize : int
            Size of a single chunk
        deterministic : bool, optional
            Whether the result should be deterministic, by default True
        order_by : list or str, optional
            List of fields that should be used to sort data. If None than data is sorted by all fields, by default None

        Examples
        --------
        >>> dsn = get_path("grizly_dev", "tests", "Chinook.sqlite")
        >>> qf = QFrame(dsn=dsn, db="sqlite", dialect="mysql", table="Playlist")
        >>> qframes = qf.cut(5, order_by="PlaylistId")
        >>> len(qframes)
        4

        Returns
        -------
        list
            List of QFrames
        """
        no_rows = self.__len__()
        qfs = []
        for chunk in range(0, no_rows, chunksize):
            qf = self.copy()
            qf = qf.window(
                offset=chunk, limit=chunksize, deterministic=deterministic, order_by=order_by
            )
            qfs.append(qf)

        return qfs

    def rearrange(self, fields: list):
        """Changes order of the columns.

        Parameters
        ----------
        fields : list or str
            Fields in list or field as a string.

        Examples
        --------
        >>> qf = QFrame(dsn="redshift_acoe", schema="grizly", table="sales")
        >>> qf = qf.rearrange(['sales', 'customer_id'])
        >>> print(qf)
        SELECT "sales",
               "customer_id"
        FROM grizly.sales

        Returns
        -------
        QFrame
        """
        if isinstance(fields, str):
            fields = [fields]

        aliased_fields = self._get_fields(aliased=True, not_selected=True)
        not_aliased_fields = self._get_fields(aliased=False, not_selected=True)

        if not set(set(aliased_fields) | set(not_aliased_fields)) >= set(fields) or len(
            not_aliased_fields
        ) != len(fields):
            raise ValueError(
                "Fields are not matching, make sure that fields are the same as in your QFrame."
            )

        fields = self._get_fields_names(fields)

        old_fields = deepcopy(self.store["select"]["fields"])
        new_fields = {}
        for field in fields:
            new_fields[field] = old_fields[field]

        self.store["select"]["fields"] = new_fields

        # self.create_sql_blocks()

        return self

    def get_fields(self, aliased: bool = False, not_selected: bool = False, **kwargs) -> List[str]:
        """Returns list of QFrame fields.

        Parameters
        ----------
        aliased : boolean
            Whether to return original names or aliases.
        not_selected : boolean
            Whether to return fields that have parameter `select=0`

        Examples
        --------
        >>> qf = QFrame(dsn="redshift_acoe", schema="grizly", table="sales")
        >>> qf.get_fields()
        ['customer_id', 'sales']

        Returns
        -------
        list
            List of field names
        """
        if not self.store:
            return []

        fields = self._get_fields(aliased=aliased, not_selected=not_selected)

        if kwargs.get("dtypes"):
            self.logger.warning(
                "Parameter dtypes in QFrame.get_dtypes is deprecated as of 0.4 and will be removed in 0.4.5."
            )
            return dict(zip(fields, self.get_dtypes()))

        return fields

    def get_dtypes(self):
        """Return list of QFrame field data types

        Examples
        --------
        >>> qf = QFrame(dsn="redshift_acoe", schema="grizly", table="sales")
        >>> qf.get_dtypes()
        ['INTEGER', 'DOUBLE PRECISION']

        Returns
        -------
        list
            List of field data dtypes
        """
        dtypes = []
        for field in self._get_fields():
            dtype = self.store["select"]["fields"][field]["dtype"]
            dtypes.append(dtype)
        return dtypes

    def to_dict(self):
        _dict = {}
        columns = self.get_fields(aliased=True)
        records = self.to_records()
        for i, column in enumerate(columns):
            column_values = [
                float(line[i]) if type(line[i]) == decimal.Decimal else line[i] for line in records
            ]
            _dict[column] = column_values
        return _dict

    def to_df(self, chunksize: int = None, verbose=False):
        """Write QFrame to DataFrame

        Returns
        -------
        DataFrame
            Data generated from sql.
        """
        d = self.to_dict()
        return DataFrame(d)

    def to_arrow(self):
        """Write QFrame to pyarrow.Table"""
        colnames = self.get_fields(aliased=True)
        # TODO: implement below more generic mapper
        coltypes = [rds_to_pyarrow(dtype) for dtype in self.get_dtypes()]
        schema = pa.schema([pa.field(name, dtype) for name, dtype in zip(colnames, coltypes)])
        self.logger.debug(f"Generating PyArrow table with schema: \n{schema}")
        _dict = self.to_dict()
        table = pa.Table.from_pydict(_dict, schema=schema)
        return table

    def copy(self):
        """Makes a copy of QFrame.

        Returns
        -------
        QFrame
        """
        return deepcopy(self)

    def _fix_types(self, mismatched: dict):
        mismatched = self._check_types()
        for col in mismatched:
            python_dtype = mismatched[col]
            sql_dtype = python_to_sql(python_dtype)
            self.store["select"]["fields"][col]["dtype"] = sql_dtype

    def _check_types(self):

        qf = self.copy().limit(100)

        expected_types = dict(zip(qf.columns, qf.dtypes))
        expected_types_mapped = {col: sql_to_python(val) for col, val in expected_types.items()}
        # this only checks the first 100 rows
        retrieved_types = {}
        d = qf.to_dict()
        for col in d:
            unique_types = {type(val) for val in d[col] if type(val) is not type(None)}
            if len(unique_types) > 1:
                raise NotImplementedError(
                    f"Multiple types detected in {col}. This is not yet handled."
                )
            if not unique_types:
                unique_types = {type(None)}
            retrieved_types[col] = list(unique_types)[0]

        mismatched_with_none = dict_diff(expected_types_mapped, retrieved_types, by="values")
        mismatched = {
            col: dtype for col, dtype in mismatched_with_none.items() if dtype is not type(None)
        }
        return mismatched

    def validate_data(self, data: Union[dict, Store]) -> Store:
        """Validates loaded data.

        Parameters
        ----------
        data : dict
            Dictionary structure holding fields, schema, table, sql information.

        Returns
        -------
        dict
            Dictionary with validated data.
        """
        return self._validate_data(deepcopy(data))

    def _get_fields_names(
        self, fields, aliased=False, not_found=False
    ) -> Union[List[str], Tuple[List[str]]]:
        """Returns a list of fields keys or fields aliases.
        Input parameters 'fields' can contain both aliased and not aliased fields

        not_found - whether to return not found fields"""
        # TODO: TO BE REFACTORED

        not_aliased_fields = self._get_fields_key_names(not_selected=True)
        aliased_fields = self._get_fields_aliases(not_selected=True)

        not_found_fields = []
        output_fields = []

        if aliased:
            for field in fields:
                if field in aliased_fields:
                    output_fields.append(field)
                elif field in not_aliased_fields:
                    output_fields.append(aliased_fields[not_aliased_fields.index(field)])
                else:
                    not_found_fields.append(field)
        else:
            for field in fields:
                if field in not_aliased_fields:
                    output_fields.append(field)
                elif field in aliased_fields:
                    output_fields.append(not_aliased_fields[aliased_fields.index(field)])
                else:
                    not_found_fields.append(field)

        if not_found_fields != []:
            self.logger.warning(f"Fields {not_found_fields} not found.")

        return output_fields if not not_found else (output_fields, not_found_fields)

    def _get_fields(self, aliased=False, not_selected=False):
        if aliased:
            return self._get_fields_aliases(not_selected=not_selected)
        else:
            return self._get_fields_key_names(not_selected=not_selected)

    def _get_fields_aliases(self, not_selected=False):
        """Return list of fields aliases (field["as"] or field key name)"""
        fields_data = self.store["select"]["fields"]
        fields_out = []

        for field in fields_data:
            if (
                not not_selected
                and "select" in fields_data[field]
                and fields_data[field]["select"] == 0
            ):
                continue
            else:
                alias = (
                    field
                    if "as" not in fields_data[field] or fields_data[field]["as"] == ""
                    else fields_data[field]["as"]
                )
                fields_out.append(alias)

        return fields_out

    def _get_fields_key_names(self, not_selected=False):
        """Return list of keys in store["select"]["fields"]"""
        fields_data = self.store["select"]["fields"]
        fields_out = []

        for field in fields_data:
            if (
                not not_selected
                and "select" in fields_data[field]
                and fields_data[field]["select"] == 0
            ):
                continue
            else:
                fields_out.append(field)

        return fields_out

    def show_duplicated_columns(self):
        """Shows duplicated columns.

        Returns
        -------
        QFrame
        """
        duplicates = self._get_duplicated_columns()

        if duplicates != {}:
            print("\033[1m", "DUPLICATED COLUMNS: \n", "\033[0m")
            for key in duplicates.keys():
                print(f"{key}:\t {duplicates[key]}\n")
            print("Use your_qframe.remove() to remove or your_qframe.rename() to rename columns.")

        else:
            self.logger.info("There are no duplicated columns.")

    def _get_duplicated_columns(self):
        columns = {}
        fields = self.store["select"]["fields"]

        for field in fields:
            alias = (
                field
                if "as" not in fields[field] or fields[field]["as"] == ""
                else fields[field]["as"]
            )
            if alias in columns.keys():
                columns[alias].append(field)
            else:
                columns[alias] = [field]

        duplicates = deepcopy(columns)
        for alias in columns.keys():
            if len(columns[alias]) == 1:
                duplicates.pop(alias)

        return duplicates

    def _validate_data(self, data) -> Store:
        if data == {}:
            raise AttributeError("Your data is empty.")

        if "select" not in data:
            raise AttributeError("Missing 'select' attribute.")

        select_data = data["select"]

        if "fields" not in select_data:
            raise AttributeError("Missing 'fields' attribute.")

        fields = select_data["fields"]

        for field_name, field_data in fields.items():
            self._validate_field(field=field_name, data=field_data)

        self._validate_key(
            key="distinct", data=select_data, func=lambda x: str(x) == "1",
        )

        self._validate_key(
            key="offset", data=select_data, func=lambda x: str(x).isdigit(),
        )

        self._validate_key(
            key="limit", data=select_data, func=lambda x: str(x).isdigit(),
        )

        return Store(data)

    def _validate_field(self, field: str, data: dict):
        if "dtype" not in data:
            self.__adjust_field_type(field, data)

        self._validate_key(key="dtype", data=data, func=lambda x: isinstance(x, str))

        self._validate_key(
            key="group_by", data=data, func=lambda x: x.upper() in self._allowed_group_by,
        )

        self._validate_key(
            key="order_by", data=data, func=lambda x: x.upper() in self._allowed_order_by,
        )

        self._validate_key(
            key="select", data=data, func=lambda x: str(x) == "0",
        )

    def __adjust_field_type(self, field: str, data: dict):
        """Replace 'custom_type' and 'type' with 'dtype' key"""
        if "custom_type" in data and data["custom_type"] != "":
            dtype = data["custom_type"].upper()
        elif data["type"] == "num":
            dtype = "FLOAT(53)"
        else:
            dtype = "VARCHAR(500)"
        data["dtype"] = dtype
        data.pop("custom_type", None)
        data.pop("type", None)
        self.logger.warning(
            f"Missing 'dtype' key in field '{field}'. "
            "Since version 0.4 of grizly uses 'dtype' key instead "
            "of 'type' and 'custom_type' keys. To update your "
            "json file please use qf.store.to_json() method."
        )

    @staticmethod
    def _validate_key(key: str, data: dict, func: Callable):

        if key in data and data[key] != "":
            value = data[key]
            if not func(value):
                raise ValueError(f"""Invalid value in {key}: '{value}'""")

    @staticmethod
    def _build_store(columns: List[str], dtypes: List[str]):
        """Create a dictionary with fields information for a Qframe

        Parameters
        ----------
        columns : list
            List of columns.
        dtypes : list
            List of data types of columns
        """
        if columns == []:
            return {}

        fields = {}

        for col, dtype in zip(columns, dtypes):
            field = {
                "as": "",
                "dtype": dtype,
                "group_by": "",
                "order_by": "",
                "select": "",
                "expression": "",
            }
            fields[col] = field

        data = {
            "select": {
                "fields": fields,
                "where": "",
                "distinct": "",
                "having": "",
                "offset": "",
                "limit": "",
            }
        }

        return Store(data)
