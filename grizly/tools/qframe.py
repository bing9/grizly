from copy import deepcopy
import decimal
from functools import partial
import json
import os
import re
from typing import Optional

import deprecation
import pandas as pd
import psutil
import pyarrow as pa
import sqlparse

from ..store import Store
from ..ui.qframe import SubqueryUI
from ..utils import (
    dict_diff,
    get_path,
    python_to_sql_dtype,
    rds_to_pyarrow_type,
    sql_to_python_dtype,
)
from .base import BaseTool
from .dialects import check_if_valid_type, mysql_to_postgres_type
from .s3 import S3
from .sqldb import SQLDB

deprecation.deprecated = partial(deprecation.deprecated, deprecated_in="0.3", removed_in="0.4")


class QFrame(BaseTool):
    """Class which builds a SQL statement

    Parameters
    ----------
    store : dict or Store
        Dictionary structure holding fields, schema, table, sql information
    dsn : str
        ODBC data source name
    table : str, optional
        Name of table
    schema : str, optional
        Name of schema
    columns : list, optional
        List of column names to retrive, NOTE: works only with table parameter
    json_path : str, optional
        Path to json file
    subquery : str, optional
        Name of the key in json file

    Examples
    --------
    >>> qf = QFrame(dsn="redshift_acoe", table="table_tutorial", schema="grizly")
    >>> print(qf)
    SELECT "col1",
           "col2",
           "col3",
           "col4"
    FROM grizly.table_tutorial
    """

    def __init__(
        self,
        store: Optional[Store] = None,
        dsn: str = None,
        schema: str = None,
        table: str = None,
        columns: list = None,
        json_path: str = None,
        subquery: str = None,
        sqldb: SQLDB = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.getfields = kwargs.get("getfields")

        engine = kwargs.get("engine")
        if engine:
            if not isinstance(engine, str):
                raise ValueError("QFrame engine is not of type: str")
            dsn = engine.split("://")[-1]
            self.logger.warning(
                "Parameter engine in QFrame is deprecated as of 0.3 and will be removed in 0.4."
                f" Please use dsn='{dsn}' instead of engine='{engine}'.",
            )

        self.sqldb = sqldb or SQLDB(dsn=dsn, **kwargs)
        data = kwargs.get("data")
        if data:
            store = data
            self.logger.warning(
                "Parameter data in QFrame is deprecated as of 0.3 and will be removed in 0.4."
                f" Please use store parameter instead.",
            )
        self.store = self._load_store(
            store=store,
            json_path=json_path,
            subquery=subquery,
            schema=schema,
            table=table,
            columns=columns,
        )

    def _load_store(
        self,
        store: Store = None,
        json_path: str = None,
        subquery: str = None,
        schema: str = None,
        table: str = None,
        columns: list = None,
    ) -> Store:
        if store:
            store = Store(store)
        elif json_path:
            store = Store().from_json(json_path=json_path, subquery=subquery)
        elif table:
            schema = schema or ""
            col_names, col_types = self.sqldb.get_columns(
                schema=schema, table=table, columns=columns, column_types=True
            )

            if col_names == []:
                raise ValueError(
                    "No columns were loaded. Please check if specified table exists and is not empty."
                )

            dict_ = initiate(schema=schema, table=table, columns=col_names, col_types=col_types)
            store = Store(dict_)
        else:
            store = Store()

        if store != Store():
            store = self.validate_data(store)
        return store

    def __str__(self):
        return self.get_sql()

    def __len__(self):
        return self.nrows

    def __getitem__(self, getfields):
        if isinstance(getfields, str):
            self.getfields = [getfields]
        elif isinstance(getfields, tuple):
            self.getfields = list(getfields)
        else:
            self.getfields = getfields
        return self

    @property
    def data(self):
        return self.store

    @property
    def ncols(self):
        ncols = len(self.get_fields())
        return ncols

    @property
    def nrows(self):
        con = self.sqldb.get_connection()
        query = f"SELECT COUNT(*) FROM ({self.get_sql()}) sq"
        if self.sqldb.db == "denodo":
            query += " CONTEXT('swap' = 'ON', 'swapsize' = '500', 'i18n' = 'us_est', 'queryTimeout' = '9000000000', 'simplify' = 'on')"
        try:
            nrows = con.execute(query).fetchone()[0]
        except:
            print(query)
            raise
        con.close()
        return nrows

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

    def create_sql_blocks(self):
        """Creates blocks which are used to generate an SQL"""
        if self.store == {}:
            self.logger.info("Your QFrame is empty.")
            return self
        else:
            self.store["select"]["sql_blocks"] = self._build_column_strings()
            return self

    def check_types(self):

        qf = self.copy().limit(100)

        expected_types = dict(zip(qf.columns, qf.dtypes))
        expected_types_mapped = {
            col: sql_to_python_dtype(val) for col, val in expected_types.items()
        }
        # this only checks the first 100 rows
        retrieved_types = {}
        d = qf.to_dict()
        for col in d:
            unique_types = {type(val) for val in d[col] if type(val) is not type(None)}
            if len(unique_types) > 1:
                raise NotImplementedError(
                    f"Multiple types detected in {col}. This is not yet handled."
                )
            retrieved_types[col] = list(unique_types)[0]

        mismatched_with_none = dict_diff(expected_types_mapped, retrieved_types, by="values")
        mismatched = {
            col: dtype for col, dtype in mismatched_with_none.items() if dtype is not type(None)
        }
        return mismatched

    def fix_types(self, mismatched: dict):
        mismatched = self.check_types()
        for col in mismatched:
            python_dtype = mismatched[col]
            sql_dtype = python_to_sql_dtype(python_dtype)
            self.store["select"]["fields"][col]["custom_type"] = sql_dtype

    def validate_data(self, data: dict):
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
        return _validate_data(deepcopy(data))

    def show_duplicated_columns(self):
        """Shows duplicated columns.

        Returns
        -------
        QFrame
        """
        duplicates = _get_duplicated_columns(self.store)

        if duplicates != {}:
            print("\033[1m", "DUPLICATED COLUMNS: \n", "\033[0m")
            for key in duplicates.keys():
                print(f"{key}:\t {duplicates[key]}\n")
            print("Use your_qframe.remove() to remove or your_qframe.rename() to rename columns.")

        else:
            self.logger.info("There are no duplicated columns.")

    def select(self, fields: list):
        """Creates a subquery that looks like "SELECT sq.col1, sq.col2 FROM (some sql) sq".

        NOTE: Selected fields will be placed in the new QFrame. Names of new fields are created
        as a concat of "sq." and alias in the parent QFrame.

        Parameters
        ----------
        fields : list or str
            Fields in list or field as a string.
            If Fields is * then Select will contain all columns

        Examples
        --------
        >>> qf = QFrame(dsn="redshift_acoe", schema="grizly", table="sales")
        >>> qf = qf.rename({"customer_id": "Id"})
        >>> print(qf)
        SELECT "customer_id" AS "Id",
               "sales"
        FROM grizly.sales
        >>> qf = qf.select(["customer_id", "sales"])
        >>> print(qf)
        SELECT sq."Id" AS "Id",
               sq."sales" AS "sales"
        FROM
          (SELECT "customer_id" AS "Id",
                  "sales"
           FROM grizly.sales) sq

        Returns
        -------
        QFrame
        """
        self.create_sql_blocks()
        sq_fields = deepcopy(self.store["select"]["fields"])
        new_fields = {}

        if isinstance(fields, str):
            if fields == "*":
                fields = self._get_fields(aliased=False, not_selected=False)
            else:
                fields = [fields]

        fields = self._get_fields_names(fields)

        for field in fields:
            if field not in sq_fields:
                self.logger.warning(f"Field {field} not found")

            elif "select" in sq_fields[field] and sq_fields[field]["select"] == 0:
                self.logger.warning(f"Field {field} is not selected in subquery.")

            else:
                if "as" in sq_fields[field] and sq_fields[field]["as"] != "":
                    alias = sq_fields[field]["as"]
                else:
                    alias = field
                new_fields[f"sq.{alias}"] = {
                    "type": sq_fields[field]["type"],
                    "as": alias,
                }
                if "custom_type" in sq_fields[field] and sq_fields[field]["custom_type"] != "":
                    new_fields[f"sq.{alias}"]["custom_type"] = sq_fields[field]["custom_type"]

        if new_fields:
            data = {"select": {"fields": new_fields}, "sq": self.store}
            self.store = Store(data)

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

    def query(self, query: str, if_exists: str = "append", operator: str = "and"):
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

        if "union" in self.store["select"]:
            self.logger.info("You can't add where clause inside union. Use select() method first.")
        else:
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

        if "union" in self.store["select"]:
            self.logger.info(
                """You can't add having clause inside union. Use select() method first.
            (The GROUP BY and HAVING clauses are applied to each individual query, not the final result set.)"""
            )

        else:
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
        self,
        type: str = "dim",
        group_by: str = "",
        order_by: str = "",
        custom_type: str = "",
        **kwargs,
    ):
        """Assigns expressions.

        Parameters
        ----------
        type : {'dim', 'num'}, optional
            Column type, by default "dim"

            * dim: VARCHAR(500)
            * num: FLOAT(53)
        group_by : {group, sum, count, min, max, avg, stddev ""}, optional
            Aggregation type, by default ""
        order_by : {'ASC','DESC'}, optional
            Sort ascending or descending, by default ''
        custom_type : str, optional
            Column type, by default ''

        Examples
        --------
        >>> qf = QFrame(dsn="redshift_acoe", schema="grizly", table="sales")
        >>> qf = qf.assign(sales_Div="sales/100", type='num')
        >>> print(qf)
        SELECT "customer_id",
               "sales",
               sales/100 AS "sales_Div"
        FROM grizly.sales

        >>> qf = QFrame(dsn="redshift_acoe", schema="grizly", table="sales")
        >>> qf = qf.assign(sales_Positive="CASE WHEN sales>0 THEN 1 ELSE 0 END")
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
        if type not in ["dim", "num"] and custom_type == "":
            raise ValueError(
                "Custom type is not provided and invalid value in type. Valid values: 'dim', 'num'."
            )
        if group_by.lower() not in [
            "group",
            "sum",
            "count",
            "min",
            "max",
            "avg",
            "stddev",
            "",
        ]:
            raise ValueError(
                "Invalid value in group_by. Valid values: 'group', 'sum', 'count', 'min', 'max', 'avg', 'stddev', ''."
            )
        if order_by.lower() not in ["asc", "desc", ""]:
            raise ValueError("Invalid value in order_by. Valid values: 'ASC', 'DESC', ''.")
        if "union" in self.store["select"]:
            self.logger.warning(
                "You can't assign expressions inside union. Use select() method first."
            )
        else:
            if kwargs is not None:
                for key in kwargs:
                    expression = kwargs[key]
                    self.store["select"]["fields"][key] = {
                        "type": type,
                        "as": key,
                        "group_by": group_by,
                        "order_by": order_by,
                        "expression": expression,
                        "custom_type": custom_type,
                    }
        return self

    def groupby(self, fields: list = None):
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
        assert (
            "union" not in self.store["select"]
        ), "You can't group by inside union. Use select() method first."

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
        if aggtype.lower() not in [
            "group",
            "sum",
            "count",
            "min",
            "max",
            "avg",
            "stddev",
        ]:
            raise ValueError(
                "Invalid value in aggtype. Valid values: 'group', 'sum', 'count', 'min', 'max', 'avg','stddev'."
            )

        if "union" in self.store["select"]:
            self.logger.warning("You can't aggregate inside union. Use select() method first.")
        else:
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
            if (
                "group_by" not in self.store["select"]["fields"][field]
                or self.store["select"]["fields"][field]["group_by"] == ""
            ):
                fields.append(field)
        return self[fields].agg("sum")

    def orderby(self, fields: list, ascending: bool = True):
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
            ascending = [ascending for item in fields]

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

                def check_if_values_are_distinct(qf, columns):
                    qf1 = qf.copy()
                    qf2 = qf.copy()
                    qf2.select(columns)
                    if len(qf1.distinct()) != len(qf2.distinct()):
                        return False
                    return True

                if not check_if_values_are_distinct(qf=self, columns=order_by):
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

        self.create_sql_blocks()

        return self

    def pivot(
        self,
        rows: list,
        columns: list,
        values: str,
        aggtype: str = "sum",
        prefix: str = None,
        sort: bool = True,
    ):
        """Reshapes QFrame to generate pivot table

        Parameters
        ----------
        rows : list
            Columns which will be grouped
        columns : list
            Columns to use to make new QFrame columns
        values : str
            Column(s) to use for populating new QFrame values
        aggtype : str, optional
            Aggregation type to perform on values, by default "sum"
        prefix : str, optional
            Prefix to add to new columns, by default None
        sort : bool, optional
            Whether to sort columns, by default True

        Returns
        -------
        QFrame
        """

        if isinstance(rows, str):
            rows = [rows]
        if isinstance(columns, str):
            columns = [columns]
        if not isinstance(values, str):
            raise ValueError("Parameter 'value' has to be of type str.")
        if values not in set(self.get_fields()) | set(self.get_fields(aliased=True)):
            raise ValueError(f"'{values}' not found in fields.")
        if aggtype not in ["sum"]:
            raise ValueError(f"Aggregation '{aggtype}' not supperted yet.")

        qf = self.copy()
        print(qf.store)
        qf.select(columns).groupby()
        if sort:
            qf.orderby(qf.get_fields())
        col_values = qf.to_records()

        values = self._get_fields_names([values], aliased=True)[0]
        columns = self._get_fields_names(columns, aliased=True)

        self.select(rows).groupby()

        for col_value in col_values:
            col_name = []
            for val in col_value:
                val = str(val)
                if not re.match("^[a-zA-Z0-9_]*$", val):
                    self.logger.warning(
                        f"Value '{val}' contains special characters. You may consider"
                        " cleaning your columns first with QFrame.assign method before pivoting."
                    )
                col_name.append(val)
            col_name = "_".join(col_name)
            if prefix is not None:
                col_name = f"{prefix}{col_name}"
            col_filter = []
            for col, val in zip(columns, col_value):
                if val is not None:
                    col_filter.append(f""""{col}"='{val}'""")
                else:
                    col_filter.append(f""""{col}" IS NULL""")
            col_filter = " AND ".join(col_filter)

            self.assign(
                **{col_name: f'CASE WHEN {col_filter} THEN "{values}" ELSE 0 END'},
                type="num",
                group_by=aggtype,
            )

        return self

    def get_fields(self, aliased: bool = False, not_selected: bool = False, dtypes=False):
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
        return dict(zip(fields, self.get_dtypes())) if dtypes else fields

    def get_dtypes(self):
        """Returns list of QFrame field data types.
        The dtypes are resolved to SQL types, e.g. 'dim' will resolved to VARCHAR(500)

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
        self.create_sql_blocks()
        dtypes = list(self.store["select"]["sql_blocks"]["types"])
        return dtypes

    def get_sql(self):
        """Overwrites the SQL statement inside the class and prints saved string.

        Examples
        --------
        >>> qf = QFrame(dsn="redshift_acoe", schema="grizly", table="sales")
        >>> print(qf.get_sql())
        SELECT "customer_id",
               "sales"
        FROM grizly.sales

        Returns
        -------
        QFrame
        """
        self.create_sql_blocks()
        return _get_sql(data=self.store, sqldb=self.sqldb)

    def create_table(
        self,
        table,
        schema="",
        char_size=500,
        dsn=None,
        sqldb=None,
        if_exists: str = "skip",
        **kwargs,
    ):
        """Creates a new empty QFrame table in database if the table doesn't exist.
        TODO: Remove engine_str, db, dsn and dialect and leave sqldb

        Parameters
        ----------
        table : str
            Name of SQL table.
        schema : str, optional
            Specify the schema.
        char_size : int, optional
            Default size of the VARCHAR field in the database column, by default 500

        Returns
        -------
        QFrame
        """
        engine_str = kwargs.get("engine_str")
        if engine_str is not None:
            dsn = engine_str.split("://")[-1]
            self.logger.warning(
                f"Parameter engine_str is deprecated as of 0.3 and will be removed in 0.4. Please use dsn='{dsn}' instead.",
            )

        sqldb = sqldb or (
            self.sqldb if dsn is None else SQLDB(dsn=dsn, logger=self.logger, **kwargs)
        )

        types = self.get_dtypes()
        if self.sqldb.dialect == "mysql" and sqldb.dialect == "postgresql":
            types = [mysql_to_postgres_type(dtype) for dtype in types]

        sqldb.create_table(
            type="base_table",
            columns=self.get_fields(aliased=True),
            types=types,
            table=table,
            schema=schema,
            char_size=char_size,
            if_exists=if_exists,
        )
        return self

    def create_external_table(self, table, schema=None, dsn=None, if_exists=None, **kwargs):
        """Creates a new empty QFrame table in database if the table doesn't exist.
        TODO: Remove engine_str, db, dsn and dialect and leave sqldb

        Parameters
        ----------
        table : str
            Name of SQL table.
        schema : str, optional
            Specify the schema.

        Returns
        -------
        QFrame
        """
        if dsn is None:
            sqldb = self.sqldb
        else:
            sqldb = SQLDB(dsn=dsn, logger=self.logger, **kwargs)

        columns = self.get_fields(aliased=True)
        types = self.get_dtypes()
        bucket = kwargs.get("bucket")
        s3_key = kwargs.get("s3_key")
        if not (("bucket" in kwargs) and ("s3_key" in kwargs)):
            msg = "'bucket' and 's3_key' parameters are required when creating an external table"
            raise ValueError(msg)

        sqldb.create_table(
            type="external_table",
            columns=columns,
            types=types,
            table=table,
            schema=schema,
            if_exists=if_exists,
            bucket=bucket,
            s3_key=s3_key,
        )
        return self

    def to_table(self, table, schema="", if_exists="fail", char_size=500):
        """Inserts values from QFrame object into given table. Name of columns in qf and table have to match each other.

        Parameters
        ----------
        table: str
            Name of SQL table
        schema: str
            Specify the schema
        if_exists : {'fail', 'replace', 'append'}, optional
            How to behave if the table already exists, by default 'fail'

            * fail: Raise a ValueError.
            * replace: Clean table before inserting new values.
            * append: Insert new values to the existing table.

        char_size : int, optional
            Default size of the VARCHAR field in the database column, by default 500

        Returns
        -------
        QFrame
        """
        self.sqldb.create_table(
            columns=self.get_fields(aliased=True),
            types=self.get_dtypes(),
            table=table,
            schema=schema,
            char_size=char_size,
        )
        self.sqldb.write_to(
            table=table,
            columns=self.get_fields(aliased=True),
            sql=self.get_sql(),
            schema=schema,
            if_exists=if_exists,
        )
        return self

    def to_records(self):
        """Writes QFrame to records.

        Examples
        --------
        >>> qf = QFrame(dsn="redshift_acoe", table="table_tutorial", schema="grizly")
        >>> qf.orderby("col1").to_records()
        [('item1', 1.3, None, 3.5), ('item2', 0.0, None, None)]

        Returns
        -------
        list
            List of rows generated by SQL.
        """

        sql = self.get_sql()
        if self.sqldb.db == "denodo":
            sql += (
                " CONTEXT('swap' = 'ON', 'swapsize' = '500', 'i18n' = 'us_est',"
                " 'queryTimeout' = '9000000000', 'simplify' = 'on')"
            )

        con = self.sqldb.get_connection()
        cursor = con.cursor()

        cursor.execute(sql)
        records = cursor.fetchall()
        cursor.close()

        con.close()

        return records

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
        """Writes QFrame to DataFrame. Uses pandas.read_sql.

        TODO: DataFarme types should correspond to types defined in QFrame data.

        Returns
        -------
        DataFrame
            Data generated from sql.
        """
        if self.debug:
            verbose = True

        sql = self.get_sql()
        if self.sqldb.db == "denodo":
            sql += " CONTEXT('swap' = 'ON', 'swapsize' = '500', 'i18n' = 'us_est', 'queryTimeout' = '9000000000', 'simplify' = 'on')"
        con = self.sqldb.get_connection()
        if verbose:
            process = psutil.Process(os.getpid())
            self.logger.info("Executing pd.read_sql()...")
            self.logger.info(
                f"Current memory usage: {process.memory_info().rss / 1024. / 1024. / 1024.:.2f}GB"
            )
        try:
            df = pd.read_sql(sql, con)
            if verbose:
                self.logger.info(df.memory_usage(deep=True))
        except:
            self.logger.exception("There was an error when running the query")
            df = pd.DataFrame()
        finally:
            con.close()
            # engine.dispose()
        if verbose:
            self.logger.info("Successfully executed pd.read_sql()")
            self.logger.info(
                f"Current memory usage: {process.memory_info().rss / 1024. / 1024. / 1024.:.2f}GB"
            )
        return df

    def to_arrow(self):
        """Writes QFrame to pyarrow.Table"""
        colnames = self.get_fields(aliased=True)
        coltypes = [rds_to_pyarrow_type(dtype) for dtype in self.get_dtypes()]
        schema = pa.schema([pa.field(name, dtype) for name, dtype in zip(colnames, coltypes)])
        self.logger.info(f"Generating PyArrow table with schema: \n{schema}")
        _dict = self.to_dict()
        table = pa.Table.from_pydict(_dict, schema=schema)
        return table

    def copy(self):
        """Makes a copy of QFrame.

        Returns
        -------
        QFrame
        """
        store = self.store.deepcopy()
        getfields = deepcopy(self.getfields)
        sqldb = self.sqldb
        logger = self.logger
        return QFrame(store=store, getfields=getfields, sqldb=sqldb, logger=logger,)

    def _get_fields_names(self, fields, aliased=False, not_found=False):
        """Returns a list of fields keys or fields aliases.
        Input parameters 'fields' can contain both aliased and not aliased fields

        not_found - whether to return not found fields"""

        not_aliased_fields = self._get_fields(aliased=False, not_selected=True)
        aliased_fields = self._get_fields(aliased=True, not_selected=True)

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
        fields_data = self.store["select"]["fields"]
        fields_out = []

        if aliased:
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
        else:
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

    def _build_column_strings(self):
        # quotes wrapping fields differ depending on database (NOT ONLY DIALECT)
        if self.sqldb.db == "mariadb":
            quote = "`"
        else:
            quote = '"'
        if self.store == {}:
            return {}

        duplicates = _get_duplicated_columns(self.store)
        assert (
            duplicates == {}
        ), f"""Some of your fields have the same aliases {duplicates}. Use your_qframe.remove() to remove or your_qframe.rename() to rename columns."""
        select_names = []
        select_aliases = []
        group_dimensions = []
        group_values = []
        order_by = []
        types = []

        fields = self.store["select"]["fields"]
        selected_fields = self._get_fields(aliased=False, not_selected=False)

        for field in fields:
            if "expression" in fields[field] and fields[field]["expression"] != "":
                expr = fields[field]["expression"]
            else:
                prefix = re.search(r"^sq\d*[.]", field)
                if prefix is not None:
                    expr = f"{prefix.group(0)}{quote}{field[len(prefix.group(0)):]}{quote}"
                else:
                    expr = f"{quote}{field}{quote}"

            alias = (
                field
                if "as" not in fields[field] or fields[field]["as"] == ""
                else fields[field]["as"]
            )
            alias = alias.replace('"', "")

            # we take either position or expression - depends if field in select
            if field in selected_fields:
                pos_nm = str(selected_fields.index(field) + 1)
            else:
                pos_nm = expr

            if "group_by" in fields[field]:
                if fields[field]["group_by"].upper() == "GROUP":
                    group_dimensions.append(pos_nm)

                elif fields[field]["group_by"].upper() in [
                    "SUM",
                    "COUNT",
                    "MAX",
                    "MIN",
                    "AVG",
                ]:
                    agg = fields[field]["group_by"]
                    expr = f"{agg}({expr})"
                    group_values.append(alias)

            if "order_by" in fields[field] and fields[field]["order_by"] != "":
                if fields[field]["order_by"].upper() == "DESC":
                    order = " DESC"
                else:
                    order = ""
                order_by.append(f"{pos_nm}{order}")

            if field in selected_fields:
                select_name = expr if expr == f"{quote}{alias}{quote}" else f'{expr} as "{alias}"'

                if "custom_type" in fields[field] and fields[field]["custom_type"] != "":
                    type = fields[field]["custom_type"].upper()
                elif fields[field]["type"] == "dim":
                    type = "VARCHAR(500)"
                elif fields[field]["type"] == "num":
                    type = "FLOAT(53)"

                select_names.append(select_name)
                select_aliases.append(alias)
                types.append(type)

        sql_blocks = {
            "select_names": select_names,
            "select_aliases": select_aliases,
            "group_dimensions": group_dimensions,
            "group_values": group_values,
            "order_by": order_by,
            "types": types,
        }

        return sql_blocks

    @deprecation.deprecated(details="Use QFrame.store.to_json instead",)
    def save_json(self, json_path: str, subquery: str = ""):
        if os.path.isfile(json_path):
            with open(json_path, "r") as f:
                json_data = json.load(f)
                if json_data == "":
                    json_data = {}
        else:
            json_data = {}

        if subquery != "":
            json_data[subquery] = self.store
        else:
            json_data = self.store

        with open(json_path, "w") as f:
            json.dump(json_data, f, indent=4)

        self.logger.info(f"Data saved in {json_path}")

    @deprecation.deprecated(details="Use QFrame(json_path=json_path, subquery=subquery) instead",)
    def from_json(self, json_path: str, subquery: str = ""):
        if json_path.startswith("s3://"):
            data = S3(url=json_path).to_serializable()
        else:
            with open(json_path, "r") as f:
                data = json.load(f)
        if data:
            if not subquery:
                self.store = Store(self.validate_data(data))
            else:
                self.store = Store(self.validate_data(data[subquery]))
        else:
            self.store = Store(data)
        return self

    @deprecation.deprecated(details="Use QFrame(json_path=json_path, subquery=subquery) instead",)
    def read_json(self, json_path, subquery=""):
        return self.from_json(json_path, subquery)

    @deprecation.deprecated(details="Use QFrame(dsn=dsn, store=data) instead",)
    def from_dict(self, data: dict):
        self.store = Store(self.validate_data(data))

        return self

    @deprecation.deprecated(details="Use QFrame(dsn=dsn, store=data) instead",)
    def read_dict(self, data):
        return self.from_dict(data=data)

    @deprecation.deprecated(details="Use QFrame(dsn=dsn, table=table, schema=schema) instead",)
    def from_table(
        self,
        table: str,
        schema: str = None,
        columns: list = None,
        json_path: str = None,
        subquery: str = None,
    ):
        if schema is None:
            schema = ""
        schema = schema if schema is not None else ""
        col_names, col_types = self.sqldb.get_columns(
            schema=schema, table=table, columns=columns, column_types=True
        )

        if col_names == []:
            raise ValueError(
                "No columns were loaded. Please check if specified table exists and is not empty."
            )

        if json_path:
            initiate(
                schema=schema,
                table=table,
                columns=col_names,
                col_types=col_types,
                subquery=subquery,
                json_path=json_path,
            )
            self.from_json(json_path=json_path, subquery=subquery)

        else:
            dict_ = initiate(schema=schema, table=table, columns=col_names, col_types=col_types)
            self.from_dict(dict_)

        return self

    @deprecation.deprecated(details="Use QFrame.to_csv, S3.from_file and S3.to_rds instead",)
    def to_rds(
        self,
        table,
        csv_path,
        schema="",
        if_exists="fail",
        sep="\t",
        use_col_names=True,
        chunksize=None,
        keep_csv=True,
        cursor=None,
        redshift_str=None,
        bucket=None,
    ):
        self.to_csv(
            csv_path=csv_path, sep=sep, chunksize=chunksize, cursor=cursor,
        )
        s3 = S3(
            file_name=os.path.basename(csv_path),
            file_dir=os.path.dirname(csv_path),
            bucket=bucket,
            redshift_str=redshift_str,
        )
        s3.from_file()
        if use_col_names:
            column_order = self.get_fields(aliased=True)
        else:
            column_order = None
        s3.to_rds(
            table=table, schema=schema, if_exists=if_exists, sep=sep, column_order=column_order,
        )
        return self

    @deprecation.deprecated(
        details="Use QFrame.to_csv or QFrame.to_df and then use SQLDB or S3 class instead",
    )
    def to_sql(
        self,
        table,
        engine,
        schema="",
        if_exists="fail",
        index=True,
        index_label=None,
        chunksize=None,
        dtype=None,
        method=None,
    ):
        df = self.to_df()
        con = self.sqldb.get_connection()

        df.to_sql(
            name=table,
            con=con,
            schema=schema,
            if_exists=if_exists,
            index=index,
            index_label=index_label,
            chunksize=chunksize,
            dtype=dtype,
            method=method,
        )
        con.close()
        return self

    @deprecation.deprecated(details="Use S3.from_file function instead",)
    def csv_to_s3(self, csv_path, s3_key=None, keep_csv=True, bucket=None):
        s3 = S3(
            file_name=os.path.basename(csv_path),
            s3_key=s3_key,
            bucket=bucket,
            file_dir=os.path.dirname(csv_path),
        )
        return s3.from_file(keep_file=keep_csv)

    @deprecation.deprecated(details="Use S3.to_rds function instead",)
    def s3_to_rds(
        self,
        table,
        s3_name,
        schema="",
        if_exists="fail",
        sep="\t",
        use_col_names=True,
        redshift_str=None,
        bucket=None,
    ):
        file_name = s3_name.split("/")[-1]
        s3_key = "/".join(s3_name.split("/")[:-1])
        s3 = S3(file_name=file_name, s3_key=s3_key, bucket=bucket, redshift_str=redshift_str)
        if use_col_names:
            column_order = self.get_fields(aliased=True)
        else:
            column_order = None
        s3.to_rds(
            table=table, schema=schema, if_exists=if_exists, sep=sep, column_order=column_order,
        )
        return self


def join(qframes=[], join_type=None, on=None, unique_col=True):
    """Joins QFrame objects. Returns QFrame.

    Name of each field is a concat of: "sq" + position of parent QFrame in qframes + "." + alias in their parent QFrame.
    If the fields have the same aliases in their parent QFrames they will have the same aliases in joined QFrame.

    By default the joined QFrame will contain all fields from the first QFrame and all fields from the other QFrames
    which are not in the first QFrame. This approach prevents duplicates. If you want to choose the columns, set unique_col=False and
    after performing join please remove fields with the same aliases or rename the aliases.

    Parameters
    ----------
    qframes : list
        List of qframes
    join_type : str or list
        Join type or a list of join types.
    on : str or list
        List of on join conditions. In case of CROSS JOIN set the condition on 0.
        NOTE: Structure of the elements of this list is very specific. You always have to use prefix "sq{qframe_position}."
        if you want to refer to the column. Check examples.
    unique_col : boolean, optional
        If True the joined QFrame will cotain all fields from the first QFrame and all fields from other QFrames which
        are not repeated. If False the joined QFrame will contain all fields from every QFrame, default True


    NOTE: Order of the elements in join_type and on list is important.

    Examples
    --------
    >>> dsn = get_path("grizly_dev", "tests", "Chinook.sqlite")
    >>> playlist_track_qf = QFrame(dsn=dsn, db="sqlite", dialect="mysql", table="PlaylistTrack", columns=["PlaylistId", "TrackId"])
    >>> print(playlist_track_qf)
    SELECT "PlaylistId",
           "TrackId"
    FROM PlaylistTrack
    >>> playlists_qf = QFrame(dsn=dsn, db="sqlite", dialect="mysql", table="Playlist", columns=["PlaylistId", "Name"])
    >>> print(playlists_qf)
    SELECT "PlaylistId",
           "Name"
    FROM Playlist
    >>> joined_qf = join(qframes=[playlist_track_qf, playlists_qf], join_type='left join', on='sq1.PlaylistId=sq2.PlaylistId')
    >>> print(joined_qf)
    SELECT sq1."PlaylistId" AS "PlaylistId",
           sq1."TrackId" AS "TrackId",
           sq2."Name" AS "Name"
    FROM
      (SELECT "PlaylistId",
              "TrackId"
       FROM PlaylistTrack) sq1
    LEFT JOIN
      (SELECT "PlaylistId",
              "Name"
       FROM Playlist) sq2 ON sq1.PlaylistId=sq2.PlaylistId

    Returns
    -------
    QFrame
    """
    assert (
        len(qframes) == len(join_type) + 1 or len(qframes) == 2 and isinstance(join_type, str)
    ), "Incorrect list size."
    assert (
        len(qframes) == 2 and isinstance(on, (int, str)) or len(join_type) == len(on)
    ), "Incorrect list size."

    data = {"select": {"fields": {}}}
    aliases = []

    iterator = 0
    for q in qframes:
        if iterator == 0:
            first_sqldb = q.sqldb
        else:
            assert first_sqldb == q.sqldb, "QFrames have different datasources."
        q.create_sql_blocks()
        iterator += 1
        data[f"sq{iterator}"] = deepcopy(q.data)
        sq = deepcopy(q.data["select"])

        for alias in sq["sql_blocks"]["select_aliases"]:
            if unique_col and alias in aliases:
                continue
            else:
                aliases.append(alias)
                for field in sq["fields"]:
                    if (
                        field == alias
                        or "as" in sq["fields"][field]
                        and sq["fields"][field]["as"] == alias
                    ):
                        data["select"]["fields"][f"sq{iterator}.{alias}"] = {
                            "type": sq["fields"][field]["type"],
                            "as": alias,
                        }
                        if (
                            "custom_type" in sq["fields"][field]
                            and sq["fields"][field]["custom_type"] != ""
                        ):
                            data["select"]["fields"][f"sq{iterator}.{alias}"]["custom_type"] = sq[
                                "fields"
                            ][field]["custom_type"]
                        break

    if isinstance(join_type, str):
        join_type = [join_type]
    if isinstance(on, (int, str)):
        on = [on]

    data["select"]["join"] = {"join_type": join_type, "on": on}

    out_qf = QFrame(store=data, sqldb=qframes[0].sqldb, logger=qframes[0].logger)

    out_qf.logger.info("Data joined successfully.")
    if not unique_col:
        out_qf.logger.warning(
            "Please remove or rename duplicated columns. Use your_qframe.show_duplicated_columns() to check duplicates."
        )
    return out_qf


def union(qframes=[], union_type=None, union_by="position"):
    """Unions QFrame objects. Returns QFrame.

    Parameters
    ----------
    qframes : list
        List of qframes
    union_type : str or list
        Type or list of union types. Valid types: 'UNION', 'UNION ALL'.
    union_by : {'position', 'name'}, optional
        How to union the qframe, by default 'position'

        * position: union by position of the field
        * name: union by the field aliases

    Examples
    --------
    >>> qf = QFrame(dsn="redshift_acoe", schema="grizly", table="sales")
    >>> qf1 = qf.copy()
    >>> qf2 = qf.copy()
    >>> qf3 = qf.copy()
    >>> q_unioned = union(qframes=[qf1, qf2, qf3], union_type=["UNION ALL", "UNION"])
    >>> print(q_unioned)
    SELECT "customer_id",
           "sales"
    FROM grizly.sales
    UNION ALL
    SELECT "customer_id",
           "sales"
    FROM grizly.sales
    UNION
    SELECT "customer_id",
           "sales"
    FROM grizly.sales

    Returns
    -------
    QFrame
    """
    if isinstance(union_type, str):
        union_type = [union_type]

    assert len(qframes) >= 2, "You have to specify at least 2 qframes to perform a union."
    assert len(qframes) == len(union_type) + 1, "Incorrect list size."
    assert set(item.upper() for item in union_type) <= {
        "UNION",
        "UNION ALL",
    }, "Incorrect union type. Valid types: 'UNION', 'UNION ALL'."
    if union_by not in {"position", "name"}:
        raise ValueError("Invalid value for union_by. Valid values: 'position', 'name'.")

    data = {"select": {"fields": {}}}

    main_qf = qframes[0]
    main_qf.create_sql_blocks()
    data["sq1"] = deepcopy(main_qf.data)
    old_fields = deepcopy(main_qf.data["select"]["fields"])
    new_fields = deepcopy(main_qf.data["select"]["sql_blocks"]["select_aliases"])
    new_types = deepcopy(main_qf.data["select"]["sql_blocks"]["types"])
    qframes.pop(0)

    iterator = 2
    for qf in qframes:
        assert main_qf.sqldb == qf.sqldb, "QFrames have different datasources."
        qf.create_sql_blocks()
        qf_aliases = qf.data["select"]["sql_blocks"]["select_aliases"]
        assert len(new_fields) == len(
            qf_aliases
        ), f"Amount of fields in {iterator}. QFrame doesn't match amount of fields in 1. QFrame."

        if union_by == "name":
            field_diff_1 = set(new_fields) - set(qf_aliases)
            field_diff_2 = set(qf_aliases) - set(new_fields)
            assert (
                field_diff_1 == set() and field_diff_2 == set()
            ), f"""Aliases {field_diff_2} not found in 1. QFrame, aliases {field_diff_1} not found in {iterator}. QFrame. Use qf.rename() to rename fields or set option union_by='position'"""
            ordered_fields = []
            for new_field in new_fields:
                fields = deepcopy(qf.data["select"]["fields"])
                for field in fields:
                    if (
                        field == new_field
                        or "as" in fields[field]
                        and fields[field]["as"] == new_field
                    ):
                        ordered_fields.append(field)
                        break
            qf.rearrange(ordered_fields)
            qf.create_sql_blocks()

        for new_field in new_fields:
            field_position = new_fields.index(new_field)
            new_type = new_types[field_position]
            qf_alias = qf.data["select"]["sql_blocks"]["select_aliases"][field_position]
            qf_type = qf.data["select"]["sql_blocks"]["types"][field_position]
            assert (
                qf_type == new_type
            ), f"Types don't match. 1. QFrame alias: {new_field} type: {new_type}, {iterator}. QFrame alias: {qf_alias} type: {qf_type}."

        data[f"sq{iterator}"] = deepcopy(qf.data)
        iterator += 1

    for field in old_fields:
        if "select" in old_fields[field] and old_fields[field]["select"] == 0:
            continue
        else:
            if "as" in old_fields[field] and old_fields[field]["as"] != "":
                alias = old_fields[field]["as"]
            else:
                alias = field

            data["select"]["fields"][alias] = {"type": old_fields[field]["type"]}
            if "custom_type" in old_fields[field] and old_fields[field]["custom_type"] != "":
                data["select"]["fields"][alias]["custom_type"] = old_fields[field]["custom_type"]

    data["select"]["union"] = {"union_type": union_type}

    out_qf = QFrame(store=data, sqldb=qframes[0].sqldb, logger=qframes[0].logger)
    out_qf.logger.info("Data unioned successfully.")

    return out_qf


def _validate_data(data):
    if data == {}:
        raise AttributeError("Your data is empty.")

    if "select" not in data:
        raise AttributeError("Missing 'select' attribute.")

    select = data["select"]

    if (
        "table" not in select
        and "join" not in select
        and "union" not in select
        and "sq" not in data
    ):
        raise AttributeError("Missing 'table' attribute.")

    if "fields" not in select:
        raise AttributeError("Missing 'fields' attribute.")

    fields = select["fields"]

    for field in fields:
        field_custom_type = ""
        for key_attr in fields[field]:
            if key_attr not in {
                "type",
                "as",
                "group_by",
                "expression",
                "select",
                "custom_type",
                "order_by",
            }:
                raise AttributeError(
                    f"""Field '{field}' has invalid attribute '{key_attr}'. Valid attributes:
                                        'type', 'as', 'group_by', 'order_by', 'expression', 'select', 'custom_type'"""
                )
        if "type" in fields[field]:
            field_type = fields[field]["type"]
            if "custom_type" in fields[field]:
                field_custom_type = fields[field]["custom_type"]
                if field_custom_type != "":
                    # if not check_if_valid_type(field_custom_type):
                    #     raise ValueError(
                    #         f"""Field '{field}' has invalid value '{field_custom_type}' in custom_type. Check valid types for Redshift tables."""
                    #     )
                    pass
                elif field_type not in ["dim", "num"] and field_custom_type == "":
                    raise ValueError(
                        f"""Field '{field}' doesn't have custom_type and has invalid value in type: '{field_type}'. Valid values: 'dim', 'num'."""
                    )
            else:
                if field_type not in ["dim", "num"]:
                    raise ValueError(
                        f"""Field '{field}' has invalid value in type: '{field_type}'. Valid values: 'dim', 'num'."""
                    )
        else:
            raise AttributeError(f"Missing type attribute in field '{field}'.")

        # if "as" in fields[field]:
        #     fields[field]["as"] = fields[field]["as"].replace(" ", "_")

        if "group_by" in fields[field] and fields[field]["group_by"] != "":
            group_by = fields[field]["group_by"]
            group_by_all_aggs = ["GROUP", "SUM", "COUNT", "MAX", "MIN", "AVG", "STDDEV"]
            group_by_numeric_aggs = group_by_all_aggs[1:]
            numeric_types = ["DOUBLE PRECISION", "INTEGER", "NUMERIC"]
            field_custom_type_has_parameter = field_custom_type.find("(") != -1
            field_custom_type_trimmed = (
                field_custom_type[: field_custom_type.find("(")]
                if field_custom_type_has_parameter
                else field_custom_type
            )

            if group_by.upper() not in group_by_all_aggs:
                raise ValueError(
                    f"""Field '{field}' has invalid value in  group_by: '{group_by}'. Valid values: '', 'group', 'sum', 'count', 'max', 'min', 'avg','stddev' """
                )

            # if group_by.upper() in group_by_numeric_aggs and (
            #     field_type != "num" and field_custom_type_trimmed not in numeric_types
            # ):
            #     raise ValueError(
            #         f"Field '{field}' has value '{field_type}' in type and value '{group_by}' in group_by. In case of aggregation type should be 'num'."
            #     )

        if "order_by" in fields[field] and fields[field]["order_by"] != "":
            order_by = fields[field]["order_by"]
            if order_by.upper() not in ["DESC", "ASC"]:
                raise ValueError(
                    f"""Field '{field}' has invalid value in order_by: '{order_by}'. Valid values: '', 'desc', 'asc'"""
                )

        if "select" in fields[field] and fields[field]["select"] != "":
            is_selected = fields[field]["select"]
            if str(is_selected) not in {"0", "0.0"}:
                raise ValueError(
                    f"""Field '{field}' has invalid value in select: '{is_selected}'.  Valid values: '', '0', '0.0'"""
                )

    if "distinct" in select and select["distinct"] != "":
        distinct = select["distinct"]
        if str(int(distinct)) != "1":
            raise ValueError(
                f"""Distinct attribute has invalid value: '{distinct}'.  Valid values: '', '1'"""
            )

    if "offset" in select and select["offset"] != "":
        offset = select["offset"]
        try:
            int(offset)
        except:
            raise ValueError(
                f"""Limit attribute has invalid value: '{offset}'.  Valid values: '', integer """
            )

    if "limit" in select and select["limit"] != "":
        limit = select["limit"]
        try:
            int(limit)
        except:
            raise ValueError(
                f"""Limit attribute has invalid value: '{limit}'.  Valid values: '', integer """
            )

    return data


def initiate(
    columns, schema, table, json_path=None, engine_str="", subquery="", col_types=None, logger=None
):
    """Creates a dictionary with fields information for a Qframe and saves the data in json file.

    Parameters
    ----------
    columns : list
        List of columns.
    schema : str
        Name of schema.
    table : str
        Name of table.
    json_path : str
        Path to output json file.
    subquery : str, optional
        Name of the query in json file. If this name already exists it will be overwritten, by default ''
    col_type : list
        List of data types of columns (in 'columns' list)
    """
    if columns == [] or table == "":
        return {}

    if json_path is not None and os.path.isfile(json_path):
        with open(json_path, "r") as f:
            json_data = json.load(f)
            if json_data == "":
                json_data = {}
    else:
        json_data = {}

    fields = {}

    if col_types is None:
        for col in columns:
            type = "num" if "amount" in col else "dim"
            field = {
                "type": type,
                "as": "",
                "group_by": "",
                "order_by": "",
                "expression": "",
                "select": "",
                "custom_type": "",
            }
            fields[col] = field

    elif isinstance(col_types, list):
        for index, col in enumerate(columns):
            custom_type = col_types[index]
            field = {
                "type": "",
                "as": "",
                "group_by": "",
                "order_by": "",
                "expression": "",
                "select": "",
                "custom_type": custom_type,
            }
            fields[col] = field

    data = {
        "select": {
            "table": table,
            "schema": schema,
            "fields": fields,
            "engine": engine_str,
            "where": "",
            "distinct": "",
            "having": "",
            "offset": "",
            "limit": "",
        }
    }

    if subquery != "":
        json_data[subquery] = data
    else:
        json_data = data

    if json_path is not None:
        with open(json_path, "w") as f:
            json.dump(json_data, f)

        print(f"Data saved in {json_path}")

    else:
        return json_data


def _get_duplicated_columns(data):
    columns = {}
    fields = data["select"]["fields"]

    for field in fields:
        alias = (
            field if "as" not in fields[field] or fields[field]["as"] == "" else fields[field]["as"]
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


def _get_sql(data, sqldb):
    if data == {}:
        return ""

    data["select"]["sql_blocks"] = QFrame(sqldb=sqldb, store=data)._build_column_strings()
    sql = ""

    if "union" in data["select"]:
        iterator = 1
        sq_data = deepcopy(data[f"sq{iterator}"])
        sql += _get_sql(sq_data, sqldb)

        for _ in data["select"]["union"]["union_type"]:
            union_type = data["select"]["union"]["union_type"][iterator - 1]
            sq_data = deepcopy(data[f"sq{iterator+1}"])
            right_table = _get_sql(sq_data, sqldb)

            sql += f" {union_type} {right_table}"
            iterator += 1

    elif "union" not in data["select"]:
        sql += "SELECT"

        if "distinct" in data["select"] and str(data["select"]["distinct"]) == "1":
            sql += " DISTINCT"

        selects = ", ".join(data["select"]["sql_blocks"]["select_names"])
        sql += f" {selects}"

        if "table" in data["select"]:
            if "schema" in data["select"] and data["select"]["schema"] != "":
                sql += " FROM {}.{}".format(data["select"]["schema"], data["select"]["table"])
            else:
                sql += " FROM {}".format(data["select"]["table"])

        elif "join" in data["select"]:
            iterator = 1
            sq_data = deepcopy(data[f"sq{iterator}"])
            left_table = _get_sql(sq_data, sqldb)
            sql += f" FROM ({left_table}) sq{iterator}"

            for _ in data["select"]["join"]["join_type"]:
                join_type = data["select"]["join"]["join_type"][iterator - 1]
                sq_data = deepcopy(data[f"sq{iterator+1}"])
                right_table = _get_sql(sq_data, sqldb)
                on = data["select"]["join"]["on"][iterator - 1]

                sql += f" {join_type} ({right_table}) sq{iterator+1}"
                if not on in {0, "0"}:
                    sql += f" ON {on}"
                iterator += 1

        elif "table" not in data["select"] and "join" not in data["select"] and "sq" in data:
            sq_data = deepcopy(data["sq"])
            sq = _get_sql(sq_data, sqldb)
            sql += f" FROM ({sq}) sq"

        if "where" in data["select"] and data["select"]["where"] != "":
            sql += " WHERE {}".format(data["select"]["where"])

        if data["select"]["sql_blocks"]["group_dimensions"] != []:
            group_names = ", ".join(data["select"]["sql_blocks"]["group_dimensions"])
            sql += f" GROUP BY {group_names}"

        if "having" in data["select"] and data["select"]["having"] != "":
            sql += " HAVING {}".format(data["select"]["having"])

    if data["select"]["sql_blocks"]["order_by"] != []:
        order_by = ", ".join(data["select"]["sql_blocks"]["order_by"])
        sql += f" ORDER BY {order_by}"

    if sqldb.dialect == "mysql":
        if "limit" in data["select"] and data["select"]["limit"] != "":
            sql += " LIMIT {}".format(data["select"]["limit"])

        if "offset" in data["select"] and data["select"]["offset"] != "":
            sql += " OFFSET {}".format(data["select"]["offset"])
    else:
        if "offset" in data["select"] and data["select"]["offset"] != "":
            sql += " OFFSET {}".format(data["select"]["offset"])

        if "limit" in data["select"] and data["select"]["limit"] != "":
            sql += " LIMIT {}".format(data["select"]["limit"])

    sql = sqlparse.format(sql, reindent=True, keyword_case="upper")
    return sql
