import pandas as pd

import re
import os
import sqlparse
from copy import deepcopy
import json
import logging
import pyarrow as pa

from .s3 import S3
from .sqldb import SQLDB, check_if_valid_type
from ..ui.qframe import SubqueryUI
from ..utils import get_path
from .extract import Extract

import deprecation
from functools import partial

deprecation.deprecated = partial(deprecation.deprecated, deprecated_in="0.3", removed_in="0.4")


def prepend_table(data, expression):
    field_regex = r"\w+[a-z]"
    escapes_regex = r"""[^"]+"|'[^']+'|and\s|or\s"""
    column_names = re.findall(field_regex, expression)
    columns_to_escape = " ".join(re.findall(escapes_regex, expression))
    for column_name in column_names:
        if column_name in columns_to_escape:
            pass
        else:
            _column_name = data["table"] + "." + column_name
            expression = expression.replace(column_name, _column_name)
            columns_to_escape += " {}".format(column_name)
    return expression


class QFrame(Extract):
    """Class which builds a SQL statement.

    Parameters
    ----------
    data : dict
        Dictionary structure holding fields, schema, table, sql information.

    engine : str
        Engine string. If empty then the engine string is "mssql+pyodbc://DenodoODBC".
        Other engine strings:

        * DenodoPROD: "mssql+pyodbc://DenodoPROD",
        * Redshift: "mssql+pyodbc://redshift_acoe",
        * MariaDB: "mssql+pyodbc://retool_dev_db"
    """

    def __init__(
        self,
        data: dict = {},
        engine: str = None,
        db: str = None,
        sql: str = None,
        getfields: list = [],
        chunksize: int = None,
        interface: str = None,
        logger=None,
    ):
        self.tool_name = "QFrame"
        self.engine = engine if engine else "mssql+pyodbc://DenodoODBC"
        if not isinstance(self.engine, str):
            raise ValueError("QFrame engine is not of type: str")
        self.data = data
        self.db = db or ("denodo" if "denodo" in self.engine.lower() else "redshift")
        self.sql = sql or ""
        self.getfields = getfields
        self.dtypes = {}
        self.chunksize = chunksize
        self.interface = interface or "sqlalchemy"
        self.logger = logger or logging.getLogger(__name__)
        super().__init__()

    def create_sql_blocks(self):
        """Creates blocks which are used to generate an SQL"""
        if self.data == {}:
            print("Your QFrame is empty.")
            return self
        else:
            self.data["select"]["sql_blocks"] = _build_column_strings(self.data)
            return self

    def validate_data(self, data):
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
        return _validate_data(data)

    def show_duplicated_columns(self):
        """Shows duplicated columns.

        Returns
        -------
        QFrame
        """
        duplicates = _get_duplicated_columns(self.data)

        if duplicates != {}:
            print("\033[1m", "DUPLICATED COLUMNS: \n", "\033[0m")
            for key in duplicates.keys():
                print(f"{key}:\t {duplicates[key]}\n")
            print("Use your_qframe.remove() to remove or your_qframe.rename() to rename columns.")

        else:
            print("There are no duplicated columns.")
        return self

    def save_json(self, json_path, subquery=""):
        """Saves QFrame.data to json file.

        Parameters
        ----------
        json_path : str
            Path to json file.
        subquery : str, optional
            Key in json file, by default ''
        """
        if os.path.isfile(json_path):
            with open(json_path, "r") as f:
                json_data = json.load(f)
                if json_data == "":
                    json_data = {}
        else:
            json_data = {}

        if subquery != "":
            json_data[subquery] = self.data
        else:
            json_data = self.data

        with open(json_path, "w") as f:
            json.dump(json_data, f, indent=4)
        print(f"Data saved in {json_path}")
        return self

    def build_subquery(self, store_path, subquery, database):
        return SubqueryUI(store_path=store_path).build_subquery(self, subquery, database)

    def from_json(self, json_path, subquery=""):
        """Reads QFrame.data from json file.

        Parameters
        ----------
        json_path : str
            Path to json file.
        subquery : str, optional
            Key in json file, by default ''

        Returns
        -------
        QFrame
        """
        with open(json_path, "r") as f:
            data = json.load(f)
            if data != {}:
                if subquery == "":
                    self.data = self.validate_data(data)
                else:
                    self.data = self.validate_data(data[subquery])
            else:
                self.data = data
        for field in self.data["select"]["fields"]:
            _type = self.data["select"]["fields"][field]["type"]
            dtype = "object"
            if _type == "num":
                dtype = "float64"
            elif _type == "dim":
                dtype == "object"
            self.dtypes[field] = dtype
        return self

    @deprecation.deprecated(details="Use QFrame.from_json instead",)
    def read_json(self, json_path, subquery=""):
        return self.from_json(json_path, subquery)

    def from_dict(self, data):
        """Reads QFrame.data from dictionary.

        Parameters
        ----------
        data : dict
            Dictionary structure holding fields, schema, table, sql information.

        Examples
        --------
        >>> data = {'select': {'fields': {'CustomerId': {'type': 'dim'}, 'Sales': {'type': 'num'}}, 'schema': 'schema', 'table': 'table'}}
        >>> qf = QFrame().from_dict(data)
        >>> print(qf)
        SELECT CustomerId,
               Sales
        FROM schema.table

        Returns
        -------
        QFrame
        """
        self.data = self.validate_data(deepcopy(data))

        return self

    @deprecation.deprecated(details="Use QFrame.from_dict instead",)
    def read_dict(self, data):
        return self.from_dict(data=data)

    def from_table(
        self, table: str, schema: str = None, columns: list = None, json_path: str = None, subquery: str = None
    ):
        """Generates QFrame by pulling columns and types from specified table.

        Parameters
        ----------
        table : str
            Name of table
        schema : str, optional
            Name of schema, by default None
        columns : list, optional
            List of column names to retrive, by default None
        json_path : str, optional
            Path to output json file, by default None
        subquery : str, optional
            Name of the query in json file. If this name already exists it will be overwritten, by default None
        
        Examples
        --------
        >>> engine_str = "mssql+pyodbc://redshift_acoe"
        >>> qf = QFrame(engine=engine_str, db="redshift", interface="pyodbc")
        >>> qf = qf.from_table(table="table_tutorial", schema="administration")
        >>> print(qf)
        SELECT col1,
               col2,
               col3,
               col4
        FROM administration.table_tutorial
        
        """
        sqldb = SQLDB(db=self.db, engine_str=self.engine, interface=self.interface)

        if schema is None:
            schema = ""
        schema = schema if schema is not None else ""
        col_names, col_types = sqldb.get_columns(schema=schema, table=table, columns=columns, column_types=True)

        if col_names == []:
            raise ValueError("No columns were loaded. Please check if specified table exists and is not empty.")

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

    def select(self, fields):
        """Creates a subquery that looks like "SELECT sq.col1, sq.col2 FROM (some sql) sq".

        NOTE: Selected fields will be placed in the new QFrame. Names of new fields are created
        as a concat of "sq." and alias in the parent QFrame.

        Examples
        --------
        >>> data = {'select': {'fields': {'CustomerId': {'type': 'dim', 'as': 'Id'}, 'Sales': {'type': 'num'}}, 'schema': 'schema', 'table': 'table'}}
        >>> qf = QFrame().from_dict(data)
        >>> print(qf)
        SELECT CustomerId AS "Id",
               Sales
        FROM schema.table
        >>> qf = qf.select(["CustomerId", "Sales"])
        >>> print(qf)
        SELECT sq.Id AS "Id",
               sq.Sales AS "Sales"
        FROM
          (SELECT CustomerId AS "Id",
                  Sales
           FROM schema.table) sq

        Parameters
        ----------
        fields : list or str
            Fields in list or field as a string.
            If Fields is * then Select will contain all columns

        Returns
        -------
        QFrame
        """
        self.create_sql_blocks()
        sq_fields = deepcopy(self.data["select"]["fields"])
        new_fields = {}

        if isinstance(fields, str):
            if fields == "*":
                fields = sq_fields
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
            data = {"select": {"fields": new_fields}, "sq": self.data}
            self.data = data

        return self

    def rename(self, fields: dict):
        """Renames columns (changes the field alias).

        Parameters
        ----------
        fields : dict
            Dictionary of columns and their new names.

        Examples
        --------
        >>> data = {'select': {'fields': {'CustomerId': {'type': 'dim'}, 'Sales': {'type': 'num'}}, 'schema': 'schema', 'table': 'table'}}
        >>> qf = QFrame().from_dict(data)
        >>> qf = qf.rename({'Sales': 'Billings'})
        >>> print(qf)
        SELECT CustomerId,
               Sales AS "Billings"
        FROM schema.table

        Returns
        -------
        QFrame
        """
        if not isinstance(fields, dict):
            raise ValueError("Fields parameter should be of type dict.")

        # fields = self._get_fields_names(fields)

        for field in fields:
            if field in self.data["select"]["fields"]:
                self.data["select"]["fields"][field]["as"] = fields[field].replace(" ", "_")
        return self

    def remove(self, fields: list):
        """Removes fields.

        Parameters
        ----------
        fields : list
            List of fields to remove.

        Examples
        --------
        >>> data = {'select': {'fields': {'CustomerId': {'type': 'dim'}, 'Sales': {'type': 'num'}}, 'schema': 'schema', 'table': 'table'}}
        >>> qf = QFrame().from_dict(data)
        >>> qf = qf.remove(['Sales'])
        >>> print(qf)
        SELECT CustomerId
        FROM schema.table

        Returns
        -------
        QFrame
        """
        if isinstance(fields, str):
            fields = [fields]

        fields = self._get_fields_names(fields)

        # if aliased:
        #     aliased_fields = self.get_fields(aliased=True)
        #     fields_diff = set(fields) - set(aliased_fields)

        #     if fields_diff != set():
        #         raise ValueError(f"Fields {fields_diff} not found.")

        #     not_aliased_fields = self.get_fields(aliased=False)
        #     fields = [not_aliased_fields[aliased_fields.index(field)] for field in fields]

        for field in fields:
            self.data["select"]["fields"].pop(field, f"Field {field} not found.")

        return self

    def distinct(self):
        """Adds DISTINCT statement.

        Examples
        --------
        >>> data = {'select': {'fields': {'CustomerId': {'type': 'dim'}, 'Sales': {'type': 'num'}}, 'schema': 'schema', 'table': 'table'}}
        >>> qf = QFrame().from_dict(data)
        >>> qf = qf.distinct()
        >>> print(qf)
        SELECT DISTINCT CustomerId,
                        Sales
        FROM schema.table

        Returns
        -------
        QFrame
        """
        self.data["select"]["distinct"] = 1

        return self

    def query(self, query, if_exists="append", operator="and"):
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
        >>> data = {'select': {'fields': {'CustomerId': {'type': 'dim'}, 'Sales': {'type': 'num'}}, 'schema': 'schema', 'table': 'table'}}
        >>> qf = QFrame().from_dict(data)
        >>> qf = qf.query("Sales != 0")
        >>> print(qf)
        SELECT CustomerId,
               Sales
        FROM schema.table
        WHERE Sales != 0

        Returns
        -------
        QFrame
        """
        if if_exists not in ["append", "replace"]:
            raise ValueError("Invalid value in if_exists. Valid values: 'append', 'replace'.")
        if operator not in ["and", "or"]:
            raise ValueError("Invalid value in operator. Valid values: 'and', 'or'.")

        if "union" in self.data["select"]:
            print("You can't add where clause inside union. Use select() method first.")
        else:
            if "where" not in self.data["select"] or self.data["select"]["where"] == "" or if_exists == "replace":
                self.data["select"]["where"] = query
            elif if_exists == "append":
                self.data["select"]["where"] += f" {operator} {query}"
        return self

    def having(self, having, if_exists="append", operator="and"):
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
        >>> data = {'select': {'fields': {'CustomerId': {'type': 'dim'}, 'Sales': {'type': 'num'}}, 'schema': 'schema', 'table': 'table'}}
        >>> qf = QFrame().from_dict(data)
        >>> qf = qf.groupby(['CustomerId'])['Sales'].agg('sum')
        >>> qf = qf.having("sum(sales)>100")
        >>> print(qf)
        SELECT CustomerId,
               sum(Sales) AS "Sales"
        FROM schema.table
        GROUP BY CustomerId
        HAVING sum(sales)>100

        Returns
        -------
        QFrame
        """
        if if_exists not in ["append", "replace"]:
            raise ValueError("Invalid value in if_exists. Valid values: 'append', 'replace'.")
        if operator not in ["and", "or"]:
            raise ValueError("Invalid value in operator. Valid values: 'and', 'or'.")

        if "union" in self.data["select"]:
            print(
                """You can't add having clause inside union. Use select() method first.
            (The GROUP BY and HAVING clauses are applied to each individual query, not the final result set.)"""
            )

        else:
            if if_exists == "replace":
                self.data["select"]["having"] = having
            else:
                if "having" in self.data["select"]:
                    self.data["select"]["having"] += f" {operator} {having}"
                else:
                    self.data["select"]["having"] = having
        return self

    def assign(self, type="dim", group_by="", order_by="", custom_type="", **kwargs):
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
        >>> data = {'select': {'fields': {'CustomerId': {'type': 'dim'}, 'Sales': {'type': 'num'}}, 'schema': 'schema', 'table': 'table'}}
        >>> qf = QFrame().from_dict(data)
        >>> qf = qf.assign(Sales_Div="Sales/100", type='num')
        >>> print(qf)
        SELECT CustomerId,
               Sales,
               Sales/100 AS "Sales_Div"
        FROM schema.table

        >>> qf = QFrame().from_dict(data)
        >>> qf = qf.assign(Sales_Positive="CASE WHEN Sales>0 THEN 1 ELSE 0 END")
        >>> print(qf)
        SELECT CustomerId,
               Sales,
               CASE
                   WHEN Sales>0 THEN 1
                   ELSE 0
               END AS "Sales_Positive"
        FROM schema.table

        Returns
        -------
        QFrame
        """
        if type not in ["dim", "num"] and custom_type == "":
            raise ValueError("Custom type is not provided and invalid value in type. Valid values: 'dim', 'num'.")
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
        if "union" in self.data["select"]:
            print("You can't assign expressions inside union. Use select() method first.")
        else:
            if kwargs is not None:
                for key in kwargs:
                    expression = kwargs[key]
                    self.data["select"]["fields"][key] = {
                        "type": type,
                        "as": key,
                        "group_by": group_by,
                        "order_by": order_by,
                        "expression": expression,
                        "custom_type": custom_type,
                    }
        return self

    def groupby(self, fields=None):
        """Adds GROUP BY statement.

        Parameters
        ----------
        fields : list or string
            List of fields or a field, if None then all fields are grouped

        Examples
        --------
        >>> data = {'select': {'fields': {'CustomerId': {'type': 'dim'}, 'Sales': {'type': 'num'}}, 'schema': 'schema', 'table': 'table'}}
        >>> qf = QFrame().from_dict(data)
        >>> qf = qf.groupby(['CustomerId'])['Sales'].agg('sum')
        >>> print(qf)
        SELECT CustomerId,
               sum(Sales) AS "Sales"
        FROM schema.table
        GROUP BY CustomerId

        Returns
        -------
        QFrame
        """
        assert "union" not in self.data["select"], "You can't group by inside union. Use select() method first."

        if isinstance(fields, str):
            fields = [fields]

        if fields is None:
            fields = self.get_fields()
        else:
            fields = self._get_fields_names(fields)

        for field in fields:
            self.data["select"]["fields"][field]["group_by"] = "group"

        return self

    def agg(self, aggtype):
        """Aggregates fields.

        Parameters
        ----------
        aggtype : {'sum', 'count', 'min', 'max', 'avg', 'stddev'}
            Aggregation type.

        Examples
        --------
        >>> qf = QFrame().from_dict(data = {'select': {'fields': {'CustomerId': {'type': 'dim'}, 'Sales': {'type': 'num'}, 'Orders': {'type': 'num'}}, 'schema': 'schema', 'table': 'table'}})
        >>> qf = qf.groupby(['CustomerId'])['Sales', 'Orders'].agg('sum')
        >>> print(qf)
        SELECT CustomerId,
               sum(Sales) AS "Sales",
               sum(Orders) AS "Orders"
        FROM schema.table
        GROUP BY CustomerId

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

        if "union" in self.data["select"]:
            print("You can't aggregate inside union. Use select() method first.")
        else:
            for field in self.getfields:
                if field in self.data["select"]["fields"]:
                    self.data["select"]["fields"][field]["group_by"] = aggtype
                else:
                    self.logger.debug("Field not found.")

        return self

    def sum(self):
        """Sums fields that have nothing in group_by key.

        Examples
        --------
        >>> qf = QFrame().from_dict(data = {'select': {'fields': {'CustomerId': {'type': 'dim'}, 'Sales': {'type': 'num'}, 'Orders': {'type': 'num'}}, 'schema': 'schema', 'table': 'table'}})
        >>> qf = qf.groupby(['CustomerId']).sum()
        >>> print(qf)
        SELECT CustomerId,
               sum(Sales) AS "Sales",
               sum(Orders) AS "Orders"
        FROM schema.table
        GROUP BY CustomerId

        Returns
        -------
        QFrame
        """
        fields = []
        for field in self.data["select"]["fields"]:
            if (
                "group_by" not in self.data["select"]["fields"][field]
                or self.data["select"]["fields"][field]["group_by"] == ""
            ):
                fields.append(field)
        return self[fields].agg("sum")

    def orderby(self, fields, ascending=True):
        """Adds ORDER BY statement.

        Parameters
        ----------
        fields : list or str
            Fields in list or field as a string.
        ascending : bool or list, optional
            Sort ascending vs. descending. Specify list for multiple sort orders, by default True

        Examples
        --------
        >>> qf = QFrame().from_dict(data = {'select': {'fields': {'CustomerId': {'type': 'dim'}, 'Sales': {'type': 'num'}}, 'schema': 'schema', 'table': 'table'}})
        >>> qf = qf.orderby(["Sales"])
        >>> print(qf)
        SELECT CustomerId,
               Sales
        FROM schema.table
        ORDER BY Sales

        >>> qf = QFrame().from_dict(data = {'select': {'fields': {'CustomerId': {'type': 'dim'}, 'Sales': {'type': 'num'}}, 'schema': 'schema', 'table': 'table'}})
        >>> qf = qf.orderby(["Sales"], ascending=False)
        >>> print(qf)
        SELECT CustomerId,
               Sales
        FROM schema.table
        ORDER BY Sales DESC

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
            if field in self.data["select"]["fields"]:
                order = "ASC" if ascending[iterator] else "DESC"
                self.data["select"]["fields"][field]["order_by"] = order
            else:
                self.logger.warning(f"Field {field} not found.")

            iterator += 1

        return self

    def limit(self, limit):
        """Adds LIMIT statement.

        Parameters
        ----------
        limit : int or str
            Number of rows to select.

        Examples
        --------
        >>> qf = QFrame().from_dict(data = {'select': {'fields': {'CustomerId': {'type': 'dim'}, 'Sales': {'type': 'num'}}, 'schema': 'schema', 'table': 'table'}})
        >>> qf = qf.limit(100)
        >>> print(qf)
        SELECT CustomerId,
               Sales
        FROM schema.table
        LIMIT 100

        Returns
        -------
        QFrame
        """
        self.data["select"]["limit"] = str(limit)

        return self

    def offset(self, offset):
        """Adds OFFSET statement.

        Parameters
        ----------
        offset : int or str
            The row from which to start the data.

        Examples
        --------
        >>> qf = QFrame().from_dict(data = {'select': {'fields': {'CustomerId': {'type': 'dim'}, 'Sales': {'type': 'num'}}, 'schema': 'schema', 'table': 'table'}})
        >>> qf = qf.offset(100)
        >>> print(qf)
        SELECT CustomerId,
               Sales
        FROM schema.table
        OFFSET 100

        Returns
        -------
        QFrame
        """
        self.data["select"]["offset"] = str(offset)

        return self

    def window(self, offset: int = None, limit: int = None, deterministic: bool = True, order_by: list = None):
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
        >>> data = {'select': {'fields': {'CustomerId': {'type': 'dim'}, 'Sales': {'type': 'num'}}, 'schema': 'schema', 'table': 'table'}}
        >>> qf = QFrame().from_dict(data)
        >>> qf = qf.window(5, 10)
        >>> print(qf)
        SELECT CustomerId,
               Sales
        FROM schema.table
        ORDER BY CustomerId,
                 Sales
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
        >>> playlists = {"select": {"fields": {"PlaylistId": {"type": "dim"}, "Name": {"type": "dim"}}, "table": "Playlist",}}
        >>> engine = "sqlite:///" + get_path("grizly_dev", "tests", "Chinook.sqlite")
        >>> qf = QFrame(engine=engine).from_dict(playlists)
        >>> qframes = qf.cut(5)
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
            qf = qf.window(offset=chunk, limit=chunksize, deterministic=deterministic, order_by=order_by)
            qfs.append(qf)

        return qfs

    def rearrange(self, fields):
        """Changes order of the columns.

        Parameters
        ----------
        fields : list or str
            Fields in list or field as a string.

        Examples
        --------
        >>> data = {'select': {'fields': {'CustomerId': {'type': 'dim'}, 'Sales': {'type': 'num'}}, 'schema': 'schema', 'table': 'table'}}
        >>> qf = QFrame().from_dict(data)
        >>> qf = qf.rearrange(['Sales', 'CustomerId'])
        >>> print(qf)
        SELECT Sales,
               CustomerId
        FROM schema.table

        Returns
        -------
        QFrame
        """
        if isinstance(fields, str):
            fields = [fields]

        old_fields = deepcopy(self.data["select"]["fields"])
        assert set(old_fields) == set(fields) and len(old_fields) == len(
            fields
        ), "Fields are not matching, make sure that fields are the same as in your QFrame."

        fields = self._get_fields_names(fields)

        new_fields = {}
        for field in fields:
            new_fields[field] = old_fields[field]

        self.data["select"]["fields"] = new_fields

        self.create_sql_blocks()

        return self

    def pivot(
        self, rows: list, columns: list, values: str, aggtype: str = "sum", prefix: str = None, sort: bool = True
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
        qf.select(columns).groupby()
        if sort:
            qf.orderby(qf.get_fields())
        col_values = qf.to_records()

        values = self._get_fields_names([values], aliased=True)[0]
        columns = self._get_fields_names(columns, aliased=True)

        self.select(rows).groupby()

        for col_value in col_values:
            col_name = "_".join([str(val) for val in col_value])
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
                **{col_name: f'CASE WHEN {col_filter} THEN "{values}" ELSE 0 END'}, type="num", group_by=aggtype
            )

        return self

    def get_fields(self, aliased=False):
        """Returns list of QFrame fields.

        Parameters
        ----------
        aliased : boolean
            Whether to return original names or aliases.

        Examples
        --------
        >>> data = {'select': {'fields': {'CustomerId': {'type': 'dim'}, 'Sales': {'type': 'num'}}, 'schema': 'schema', 'table': 'table'}}
        >>> qf = QFrame().from_dict(data)
        >>> qf.get_fields()
        ['CustomerId', 'Sales']

        Returns
        -------
        list
            List of field names
        """
        if self.data:
            return self._get_fields(aliased=aliased)
        else:
            return []

    def get_dtypes(self):
        """Returns list of QFrame field data types.
        The dtypes are resolved to SQL types, e.g. 'dim' will resolved to VARCHAR(500)

        Examples
        --------
        >>> data = {'select': {'fields': {'CustomerId': {'type': 'dim'}, 'Sales': {'type': 'num'}}, 'schema': 'schema', 'table': 'table'}}
        >>> qf = QFrame().from_dict(data)
        >>> qf.get_dtypes()
        ['VARCHAR(500)', 'FLOAT(53)']

        Returns
        -------
        list
            List of field data dtypes
        """
        self.create_sql_blocks()
        dtypes = self.data["select"]["sql_blocks"]["types"]
        return dtypes

    def get_sql(self, print_sql=True):
        """Overwrites the SQL statement inside the class and prints saved string.

        Examples
        --------
        >>> data = {'select': {'fields': {'CustomerId': {'type': 'dim'}, 'Sales': {'type': 'num'}}, 'schema': 'schema', 'table': 'table'}}
        >>> qf = QFrame().from_dict(data)
        >>> print(qf.get_sql())
        SELECT CustomerId,
               Sales
        FROM schema.table

        Returns
        -------
        QFrame
        """
        self.create_sql_blocks()
        self.sql = _get_sql(data=self.data, db=self.db)
        return self.sql

    def create_table(self, table, schema="", char_size=500, engine_str=None):
        """Creates a new empty QFrame table in database if the table doesn't exist.

        Parameters
        ----------
        table : str
            Name of SQL table.
        schema : str, optional
            Specify the schema.
        char_size : int, optional
            Default size of the VARCHAR field in the database column, by default 500
        engine_str : str, optional
            Engine string, by default "mssql+pyodbc://redshift_acoe"

        Returns
        -------
        QFrame
        """
        engine_str = engine_str or self.engine
        self.create_sql_blocks()
        sqldb = SQLDB(db=self.db, engine_str=engine_str, interface=self.interface)
        sqldb.create_table(
            columns=self.get_fields(aliased=True),
            types=self.get_dtypes(),
            table=table,
            schema=schema,
            char_size=char_size,
        )
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
        self.create_sql_blocks()
        sqldb = SQLDB(db=self.db, engine_str=self.engine, interface=self.interface)
        sqldb.create_table(
            columns=self.get_fields(aliased=True),
            types=self.get_dtypes(),
            table=table,
            schema=schema,
            char_size=char_size,
        )
        sqldb.write_to(
            table=table, columns=self.get_fields(aliased=True), sql=self.get_sql(), schema=schema, if_exists=if_exists,
        )
        return self

    def to_records(self):
        """Writes QFrame to records.

        Examples
        --------
        >>> engine_str = "mssql+pyodbc://redshift_acoe"
        >>> qf = QFrame(engine=engine_str, db="redshift", interface="pyodbc")
        >>> qf = qf.from_table(table="table_tutorial", schema="administration")
        >>> qf.to_records()
        [('item1', 1.3, None, 3.5), ('item2', 0.0, None, None)]

        Returns
        -------
        list
            List of rows generated by SQL.
        """

        sql = self.get_sql()
        if self.db == "denodo":
            sql += (
                " CONTEXT('swap' = 'ON', 'swapsize' = '500', 'i18n' = 'us_est',"
                " 'queryTimeout' = '9000000000', 'simplify' = 'on')"
            )

        sqldb = SQLDB(db=self.db, engine_str=self.engine, interface=self.interface, logger=self.logger)
        con = sqldb.get_connection()
        cursor = con.cursor()

        cursor.execute(sql)
        records = cursor.fetchall()
        cursor.close()

        con.close()

        return records

    def to_df(self, db="redshift", chunksize: int = None):
        """Writes QFrame to DataFrame. Uses pandas.read_sql.

        TODO: DataFarme types should correspond to types defined in QFrame data.

        Parameters
        ---------
        db : not really used but has to be provided
        
        Returns
        -------
        DataFrame
            Data generated from sql.
        """
        sql = self.get_sql()
        if "denodo" in self.engine.lower():
            sql += " CONTEXT('swap' = 'ON', 'swapsize' = '500', 'i18n' = 'us_est', 'queryTimeout' = '9000000000', 'simplify' = 'on')"
        # sqldb = SQLDB(db=db, engine_str=self.engine, interface=self.interface, logger=self.logger)
        # con = sqldb.get_connection()
        # offset = 0
        # dfs = []
        # if chunksize:
        #     if not "limit" in sql.lower():  # respect existing LIMIT
        #         while True:
        #             chunk_sql = sql + f"\nOFFSET {offset} LIMIT {self.chunksize}"
        #             chunk_df = pd.read_sql(chunk_sql, con)
        #             dfs.append(chunk_df)
        #             offset += chunksize
        #             if len(dfs[-1]) < chunksize:
        #                 break
        #         df = pd.concat(dfs)
        #     else:
        #         self.logger.warning(f"LIMIT already exists in query. Chunksize will not be applied")
        con = SQLDB(db=self.db, engine_str=self.engine, interface=self.interface).get_connection()
        try:
            df = pd.read_sql(sql, con)
        except:
            self.logger.warning("Query returned no results")
            df = pd.DataFrame()
        # df = read_sql(sql=sql, con=con)
        # import io
        # from sqlalchemy import create_engine
        # copy_sql = f"COPY ({sql}) TO STDOUT WITH CSV HEADER"
        # engine_str = "mssql+pyodbc://DenodoPROD"
        # engine = create_engine(engine_str)
        # conn = engine.raw_connection()
        # cur = conn.cursor()
        # store = io.StringIO()
        # cur.copy_expert(copy_sql, store)
        # store.seek(0)
        # df = read_csv(store)
        # self.df = df
        finally:
            con.close()
            # engine.dispose()
        return df

    def to_arrow(self, db="redshift", debug=False):
        """Writes QFrame to pyarrow.Table"""

        sql = self.get_sql()
        sqldb = SQLDB(db=db, engine_str=self.engine, interface="turbodbc", logger=self.logger)
        con = sqldb.get_connection()
        cursor = con.cursor()
        cursor.execute(sql)
        rowcount = cursor.rowcount
        batches = cursor.fetcharrowbatches(strings_as_dictionary=True)  # string_as.. - similar to pd.Categorical
        arrow_table = pa.concat_tables(batches)
        cursor.close()
        con.close()
        if debug:
            return arrow_table, rowcount
        return arrow_table

    @deprecation.deprecated(details="Use QFrame.to_csv or QFrame.to_df and then use SQLDB or S3 class instead",)
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
        sqldb = SQLDB(db=self.db, engine_str=self.engine, interface=self.interface)
        con = sqldb.get_connection()

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
        s3 = S3(file_name=os.path.basename(csv_path), s3_key=s3_key, bucket=bucket, file_dir=os.path.dirname(csv_path),)
        return s3.from_file(keep_file=keep_csv)

    @deprecation.deprecated(details="Use S3.to_rds function instead",)
    def s3_to_rds(
        self, table, s3_name, schema="", if_exists="fail", sep="\t", use_col_names=True, redshift_str=None, bucket=None,
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

    def copy(self):
        """Makes a copy of QFrame.

        Returns
        -------
        QFrame
        """
        data = deepcopy(self.data)
        engine = self.engine
        db = self.db
        sql = self.sql
        getfields = deepcopy(self.getfields)
        logger = self.logger
        interface = self.interface
        return QFrame(data=data, engine=engine, db=db, sql=sql, getfields=getfields, logger=logger, interface=interface)

    def __str__(self):
        sql = self.get_sql()
        return sql

    def __len__(self):
        con = SQLDB(db=self.db, engine_str=self.engine, interface=self.interface).get_connection()
        query = f"SELECT COUNT(*) FROM ({self.get_sql()}) sq"
        try:
            no_rows = con.execute(query).fetchval()
        except:
            no_rows = con.execute(query).fetchone()[0]
        return no_rows

    def __getitem__(self, getfields):
        if isinstance(getfields, str):
            self.getfields = [getfields]
        elif isinstance(getfields, tuple):
            self.getfields = list(getfields)
        else:
            self.getfields = getfields
        return self

    def _get_fields_names(self, fields, aliased=False):
        """Returns a list of fields keys or fields aliases. Input parameters 'fields' can contain both aliased and not aliased fields"""
        not_aliased_fields = self._get_fields(aliased=False)
        aliased_fields = self._get_fields(aliased=True)

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

        return output_fields

    def _get_fields(self, aliased=False):
        fields_data = self.data["select"]["fields"]
        fields_out = []

        if aliased:
            for field in fields_data:
                alias = (
                    field
                    if "as" not in fields_data[field] or fields_data[field]["as"] == ""
                    else fields_data[field]["as"]
                )
                fields_out.append(alias)
        else:
            for field in fields_data:
                fields_out.append(field)

        return fields_out


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

    TODO: Add validations on engines. QFarmes engines have to be the same.

    Examples
    --------
    >>> playlist_track = {"select": {"fields":{"PlaylistId": {"type" : "dim"}, "TrackId": {"type" : "dim"}}, "table" : "PlaylistTrack"}}
    >>> playlist_track_qf = QFrame().from_dict(playlist_track)
    >>> print(playlist_track_qf)
    SELECT PlaylistId,
           TrackId
    FROM PlaylistTrack
    >>> playlists = {"select": {"fields": {"PlaylistId": {"type" : "dim"}, "Name": {"type" : "dim"}}, "table" : "Playlist"}}
    >>> playlists_qf = QFrame().from_dict(playlists)
    >>> print(playlists_qf)
    SELECT PlaylistId,
           Name
    FROM Playlist
    >>> joined_qf = join(qframes=[playlist_track_qf, playlists_qf], join_type='left join', on='sq1.PlaylistId=sq2.PlaylistId')
    Data joined successfully.
    >>> print(joined_qf)
    SELECT sq1.PlaylistId AS "PlaylistId",
           sq1.TrackId AS "TrackId",
           sq2.Name AS "Name"
    FROM
      (SELECT PlaylistId,
              TrackId
       FROM PlaylistTrack) sq1
    LEFT JOIN
      (SELECT PlaylistId,
              Name
       FROM Playlist) sq2 ON sq1.PlaylistId=sq2.PlaylistId

    Returns
    -------
    QFrame
    """
    assert (
        len(qframes) == len(join_type) + 1 or len(qframes) == 2 and isinstance(join_type, str)
    ), "Incorrect list size."
    assert len(qframes) == 2 and isinstance(on, (int, str)) or len(join_type) == len(on), "Incorrect list size."

    data = {"select": {"fields": {}}}
    aliases = []

    iterator = 0
    for q in qframes:
        if iterator == 0:
            first_engine = q.engine
            first_db = q.db
        else:
            assert first_engine == q.engine, "QFrames have different engine strings."
            assert first_db == q.db, "QFrames have different db parameters."
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
                    if field == alias or "as" in sq["fields"][field] and sq["fields"][field]["as"] == alias:
                        data["select"]["fields"][f"sq{iterator}.{alias}"] = {
                            "type": sq["fields"][field]["type"],
                            "as": alias,
                        }
                        if "custom_type" in sq["fields"][field] and sq["fields"][field]["custom_type"] != "":
                            data["select"]["fields"][f"sq{iterator}.{alias}"]["custom_type"] = sq["fields"][field][
                                "custom_type"
                            ]
                        break

    if isinstance(join_type, str):
        join_type = [join_type]
    if isinstance(on, (int, str)):
        on = [on]

    data["select"]["join"] = {"join_type": join_type, "on": on}

    print("Data joined successfully.")
    if not unique_col:
        print(
            "Please remove or rename duplicated columns. Use your_qframe.show_duplicated_columns() to check duplicates."
        )
    return QFrame(data=data, engine=qframes[0].engine, db=qframes[0].db)


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
    >>> qf = QFrame().from_dict(data = {'select': {'fields': {'CustomerId': {'type': 'dim'}, 'Sales': {'type': 'num'}}, 'schema': 'schema', 'table': 'table'}})
    >>> qf1 = qf.copy()
    >>> qf2 = qf.copy()
    >>> qf3 = qf.copy()
    >>> q_unioned = union(qframes=[qf1, qf2, qf3], union_type=["UNION ALL", "UNION"])
    Data unioned successfully.
    >>> print(q_unioned)
    SELECT CustomerId,
           Sales
    FROM schema.table
    UNION ALL
    SELECT CustomerId,
           Sales
    FROM schema.table
    UNION
    SELECT CustomerId,
           Sales
    FROM schema.table

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
        assert main_qf.engine == qf.engine, "QFrames have different engine strings."
        assert main_qf.db == qf.db, "QFrames have different db parameters."
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
                    if field == new_field or "as" in fields[field] and fields[field]["as"] == new_field:
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

    print("Data unioned successfully.")
    return QFrame(data=data, engine=qframes[0].engine, db=qframes[0].db)


def _validate_data(data):
    if data == {}:
        raise AttributeError("Your data is empty.")

    if "select" not in data:
        raise AttributeError("Missing 'select' attribute.")

    select = data["select"]

    if "table" not in select and "join" not in select and "union" not in select and "sq" not in data:
        raise AttributeError("Missing 'table' attribute.")

    if "fields" not in select:
        raise AttributeError("Missing 'fields' attribute.")

    fields = select["fields"]

    for field in fields:
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

        if "as" in fields[field]:
            fields[field]["as"] = fields[field]["as"].replace(" ", "_")

        if "group_by" in fields[field] and fields[field]["group_by"] != "":
            group_by = fields[field]["group_by"]
            if group_by.upper() not in [
                "GROUP",
                "SUM",
                "COUNT",
                "MAX",
                "MIN",
                "AVG",
                "STDDEV",
            ]:
                raise ValueError(
                    f"""Field '{field}' has invalid value in  group_by: '{group_by}'. Valid values: '', 'group', 'sum', 'count', 'max', 'min', 'avg','stddev' """
                )
            elif group_by.upper() in ["SUM", "COUNT", "MAX", "MIN", "AVG", "STDDEV"] and field_type != "num":
                raise ValueError(
                    f"Field '{field}' has value '{field_type}' in type and value '{group_by}' in group_by. In case of aggregation type should be 'num'."
                )

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
            raise ValueError(f"""Distinct attribute has invalid value: '{distinct}'.  Valid values: '', '1'""")

    if "offset" in select and select["offset"] != "":
        offset = select["offset"]
        try:
            int(offset)
        except:
            raise ValueError(f"""Limit attribute has invalid value: '{offset}'.  Valid values: '', integer """)

    if "limit" in select and select["limit"] != "":
        limit = select["limit"]
        try:
            int(limit)
        except:
            raise ValueError(f"""Limit attribute has invalid value: '{limit}'.  Valid values: '', integer """)

    return data


def initiate(columns, schema, table, json_path=None, engine_str="", subquery="", col_types=None):
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
        alias = field if "as" not in fields[field] or fields[field]["as"] == "" else fields[field]["as"]
        if alias in columns.keys():
            columns[alias].append(field)
        else:
            columns[alias] = [field]

    duplicates = deepcopy(columns)
    for alias in columns.keys():
        if len(columns[alias]) == 1:
            duplicates.pop(alias)

    return duplicates


def _build_column_strings(data):
    if data == {}:
        return {}

    duplicates = _get_duplicated_columns(data)
    assert (
        duplicates == {}
    ), f"""Some of your fields have the same aliases {duplicates}. Use your_qframe.remove() to remove or your_qframe.rename() to rename columns."""
    select_names = []
    select_aliases = []
    group_dimensions = []
    group_values = []
    order_by = []
    types = []

    fields = data["select"]["fields"]

    for field in fields:
        expr = (
            field
            if "expression" not in fields[field] or fields[field]["expression"] == ""
            else fields[field]["expression"]
        )
        alias = field if "as" not in fields[field] or fields[field]["as"] == "" else fields[field]["as"]
        alias = alias.replace('"', "")

        if "group_by" in fields[field]:
            if fields[field]["group_by"].upper() == "GROUP":
                prefix = re.search(r"^sq\d*[.]", field)
                if prefix is not None:
                    group_dimensions.append(field[len(prefix.group(0)) :])
                else:
                    group_dimensions.append(field)

            elif fields[field]["group_by"] == "":
                pass

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

        if "select" not in fields[field] or "select" in fields[field] and fields[field]["select"] == "":
            select_name = field if expr == alias else f'{expr} as "{alias}"'

            if "custom_type" in fields[field] and fields[field]["custom_type"] != "":
                type = fields[field]["custom_type"].upper()
            elif fields[field]["type"] == "dim":
                type = "VARCHAR(500)"
            elif fields[field]["type"] == "num":
                type = "FLOAT(53)"

            if "order_by" in fields[field] and fields[field]["order_by"] != "":
                if fields[field]["order_by"].upper() == "DESC":
                    order = fields[field]["order_by"]
                elif fields[field]["order_by"].upper() == "ASC":
                    order = ""
                order_by.append(f"{alias} {order}")

            select_names.append(select_name)
            select_aliases.append(alias)
            types.append(type)
        else:
            pass

    sql_blocks = {
        "select_names": select_names,
        "select_aliases": select_aliases,
        "group_dimensions": group_dimensions,
        "group_values": group_values,
        "order_by": order_by,
        "types": types,
    }

    return sql_blocks


def _get_sql(data, db):
    if data == {}:
        return ""

    data["select"]["sql_blocks"] = _build_column_strings(data)
    sql = ""

    if "union" in data["select"]:
        iterator = 1
        sq_data = deepcopy(data[f"sq{iterator}"])
        sql += _get_sql(sq_data, db)

        for union in data["select"]["union"]["union_type"]:
            union_type = data["select"]["union"]["union_type"][iterator - 1]
            sq_data = deepcopy(data[f"sq{iterator+1}"])
            right_table = _get_sql(sq_data, db)

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
            left_table = _get_sql(sq_data, db)
            sql += f" FROM ({left_table}) sq{iterator}"

            for join in data["select"]["join"]["join_type"]:
                join_type = data["select"]["join"]["join_type"][iterator - 1]
                sq_data = deepcopy(data[f"sq{iterator+1}"])
                right_table = _get_sql(sq_data, db)
                on = data["select"]["join"]["on"][iterator - 1]

                sql += f" {join_type} ({right_table}) sq{iterator+1}"
                if not on in {0, "0"}:
                    sql += f" ON {on}"
                iterator += 1

        elif "table" not in data["select"] and "join" not in data["select"] and "sq" in data:
            sq_data = deepcopy(data["sq"])
            sq = _get_sql(sq_data, db)
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

    if db in ["sqlite", "mariadb"]:
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


if __name__ == "__main__":
    import doctest

    doctest.testmod()
