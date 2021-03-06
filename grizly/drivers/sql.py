from copy import deepcopy
from functools import partial
import json
import os
import re
from typing import Any, List, Literal, Literal, Tuple, Union
import warnings

import deprecation
import sqlparse

from .. import types
from ..sources.sources_factory import Source
from ..store import Store
from ..utils.deprecation import deprecated_params
from ..utils.functions import isinstance2
from .base import BaseDriver

deprecation.deprecated = partial(deprecation.deprecated, deprecated_in="0.4", removed_in="0.4.5")
deprecated_params = partial(deprecated_params, deprecated_in="0.4", removed_in="0.4.5")


class SQLDriver(BaseDriver):
    """Class that builds select statement upon a table in relational database.

    Parameters
    ----------
    dsn : str, optional
        Datasource name that should be read from config, by default None
    schema : str, optional
        Schema name, by default None
    table : str, optional
        Table name, by default None
    columns : list, optional
        Columns that should be retrived, if None then all columns are retrived, by default None
    source : Source, optional
        Instead of dsn Source class can be specified, by default None

    Examples
    --------
    >>> from grizly import QFrame
    >>> qf = QFrame(dsn="redshift_acoe", schema="grizly", table="sales")
    >>> print(qf)
    SELECT "customer_id",
           "sales"
    FROM grizly.sales

    See Also
    --------
    SQLDriver.from_dict
    SQLDriver.from_json

    """

    def __init__(
        self,
        dsn: str = None,
        schema: str = None,
        table: str = None,
        columns: List[str] = None,
        source: types.Source = None,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.source = source or Source(dsn=dsn, **kwargs)

        if not self.store and table:
            self.store = self._load_store_from_table(schema=schema, table=table, columns=columns)

    def __str__(self):
        return self.get_sql()

    # def _load_store_from_table(
    #     self, schema: str = None, table: str = None, columns: list = None,
    # ) -> Store:
    #     table = self.source.table(table, schema=schema)

    #     if not table:
    #         raise ValueError(f"Table {table} does not exist")
    #     if not table.columns:
    #         raise ValueError(f"Table {table} is empty")

    #     _dict = self._build_store(columns=table.columns, dtypes=table.types)
    #     _dict["select"]["table"] = table
    #     _dict["select"]["schema"] = schema or ""

    #     return Store(_dict)

    def _load_store_from_table(
        self, schema: str = None, table: str = None, columns: list = None,
    ) -> Store:
        schema = schema or ""
        col_names, col_types = self.source.get_columns(
            schema=schema, table=table, columns=columns, column_types=True
        )

        if col_names == []:
            raise ValueError(
                "No columns were loaded. Please check if specified table exists and is not empty."
            )

        _dict = self._build_store(columns=col_names, dtypes=col_types)
        _dict["select"]["table"] = table
        _dict["select"]["schema"] = schema

        return Store(_dict)

    # TODO: probably from_table method should be deprecated
    def from_table(
        self,
        table: str,
        schema: str = None,
        columns: List[str] = None,
        json_path: str = None,
        subquery: str = None,
    ):
        """Generate QFrame by pulling columns and types from specified table.

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
        """
        self.store = self._load_store_from_table(table=table, schema=schema, columns=columns)

        if json_path:
            self.store.to_json(json_path=json_path, subquery=subquery)

        return self

    @property
    def nrows(self):
        query = f"SELECT COUNT(*) FROM ({self.get_sql()}) sq"
        records = self.source._fetch_records(query)
        if records:
            nrows = records[0][0]
        else:
            nrows = 0
        return nrows

    def _create_sql_blocks(self):
        """Create blocks which are used to generate an SQL."""
        if self.store == {}:
            self.logger.info("Your QFrame is empty.")
            return self
        else:
            self.store["select"]["sql_blocks"] = self._build_column_strings()
            return self

    def select(self, fields: Union[str, List[str]]):
        """Create a subquery that looks like "SELECT sq.col1, sq.col2 FROM (some sql) sq".

        NOTE: Selected fields will be placed in the new QFrame. Names of new fields are created
        as a concat of "sq." and alias in the parent QFrame.

        Parameters
        ----------
        fields : list or str
            Fields in list or field as a string.
            If Fields is * then Select will contain all columns

        Examples
        --------
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
        self._create_sql_blocks()
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
                    "dtype": sq_fields[field]["dtype"],
                    "as": alias,
                }

        if new_fields:
            data = {"select": {"fields": new_fields}, "sq": self.store}
            self.store = Store(data)

        return self

    def pivot(
        self,
        rows: Union[str, List[str]],
        columns: Union[str, List[str]],
        values: str,
        aggtype: str = "sum",
        prefix: str = None,
        sort: bool = True,
    ):
        """Reshape QFrame to generate a pivot table.

        Parameters
        ----------
        rows : str or list
            Columns which will be grouped
        columns : str or list
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
        col_values = qf.to_records()
        if sort:
            col_values = sorted(col_values)

        value = self._get_fields_names([values], aliased=True)[0]
        value_type = self.store["select"]["fields"][self._get_fields_names([values])[0]]["dtype"]
        columns = self._get_fields_names(columns, aliased=True)

        self.select(rows).groupby()

        for col_value in col_values:
            col_name = []
            for val in col_value:
                val = str(val)
                if not re.match("^[a-zA-Z0-9_]*$", val):
                    warnings.warn(
                        f"Value '{val}' contains special characters. You may consider"
                        " cleaning your columns first with QFrame.assign method before pivoting.",
                        UserWarning,
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
                **{col_name: f'CASE WHEN {col_filter} THEN "{value}" ELSE 0 END'},
                dtype=value_type,
                group_by=aggtype,
            )

        return self

    def get_sql(self):
        """Get SQL statement generated by the object.

        Examples
        --------
        >>> print(qf.get_sql())
        SELECT "customer_id",
               "sales"
        FROM grizly.sales

        Returns
        -------
        QFrame
        """
        self._create_sql_blocks()
        return self._get_sql(data=self.store, sqldb=self.source)

    @property
    def sql(self):
        """Query generated by QFrame"""
        return self.get_sql()

    @deprecated_params({"char_size": None, "sqldb": "output_source"})
    def create_table(
        self,
        table: str,
        schema: str = None,
        dsn: str = None,
        output_source: types.Source = None,
        if_exists: Literal["fail", "skip", "drop"] = "skip",
        **source_kwargs,
    ):
        """Create a new empty table in database based on QFrame's fields and types.

        Parameters
        ----------
        table : str
            Name of SQL table.
        schema : str, optional
            Specify the schema.
        if_exists : {'fail', 'skip', 'drop'}, optional
            How to behave if the table already exists, by default 'skip'

            * skip: Leave old table.
            * fail: Raise a ValueError.
            * drop: Drop table before creating new one.

        Returns
        -------
        QFrame
        """

        output_source = output_source or (
            self.source if dsn is None else Source(dsn=dsn, logger=self.logger, **source_kwargs)
        )

        dtypes = self.get_dtypes()
        if self.source.dialect != output_source.dialect:
            dtypes = output_source.map_types(dtypes, to=output_source.dialect)

        output_source.create_table(
            type="base_table",
            columns=self.get_fields(aliased=True),
            types=dtypes,
            table=table,
            schema=schema,
            if_exists=if_exists,
        )
        return self

    def create_external_table(
        self,
        table: str,
        schema: str = None,
        dsn: str = None,
        output_source: types.Source = None,
        if_exists: str = None,
        bucket: str = None,
        s3_key: str = None,
        s3_url: str = None,
        **source_kwargs,
    ):
        """Create a new empty external table based on QFrame's fields and types.

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
        output_source = output_source or (
            self.source if dsn is None else Source(dsn=dsn, logger=self.logger, **source_kwargs)
        )

        if not isinstance2(output_source, types.Redshift):
            msg = f"Writing to external tables is only supported in Redshift. Got {output_source}"
            raise NotImplementedError(msg)

        columns = self.get_fields(aliased=True)
        mapped_types = self.source.map_types(self.get_dtypes(), to=output_source.dialect)
        output_source.create_table(
            table_type="external",
            columns=columns,
            types=mapped_types,
            table=table,
            schema=schema,
            if_exists=if_exists,
            bucket=bucket,
            s3_key=s3_key,
            s3_url=s3_url,
        )
        return self

    def to_table(
        self,
        table: str,
        schema: str = "",
        if_exists: Literal["fail", "replace", "append", "drop"] = "fail",
        char_size=500,
    ):
        """Insert values generated by QFrame's query into given table.

        Names of columns in qf and table have to match each other.

        Parameters
        ----------
        table: str
            Name of SQL table
        schema: str
            Specify the schema
        if_exists : {'fail', 'replace', 'append', 'drop'}, optional
            How to behave if the table already exists, by default 'fail'

            * fail: Raise a ValueError.
            * replace: Clean table before inserting new values.
            * append: Insert new values to the existing table.
            * drop: Drop table before inserting values.

        Returns
        -------
        QFrame
        """
        if if_exists in ("fail", "drop"):
            if_exists_create = if_exists
            if_exists_write = "replace"
        else:
            if_exists_create = "skip"
            if_exists_write = if_exists

        self.source.create_table(
            columns=self.get_fields(aliased=True),
            types=self.get_dtypes(),
            table=table,
            schema=schema,
            char_size=char_size,
            if_exists=if_exists_create,
        )
        self.source.write_to(
            table=table,
            columns=self.get_fields(aliased=True),
            sql=self.get_sql(),
            schema=schema,
            if_exists=if_exists_write,
        )
        return self

    def to_records(self) -> List[Tuple[Any]]:
        """Write QFrame's result to records.

        Examples
        --------
        >>> from grizly import QFrame
        >>> qf = QFrame(dsn="redshift_acoe", table="table_tutorial", schema="grizly")
        >>> qf.orderby("col1").to_records()
        [('item1', 1.3, None, 3.5), ('item2', 0.0, None, None)]

        Returns
        -------
        list
            List of rows generated by SQL.
        """

        sql = self.get_sql()
        records = self.source._fetch_records(sql)

        return records

    def _validate_store(self, store):

        if not store:
            return

        store = super()._validate_store(store=store)

        select_data = store["select"]
        if (
            "table" not in select_data
            and "join" not in select_data
            and "union" not in select_data
            and "sq" not in store
        ):
            raise AttributeError("Missing 'table' attribute.")

        return store

    @classmethod
    def _get_sql(cls, data, sqldb):
        if data == {}:
            return ""

        data["select"]["sql_blocks"] = cls(source=sqldb, store=data)._build_column_strings()
        sql = ""

        if "union" in data["select"]:
            iterator = 1
            sq_data = deepcopy(data[f"sq{iterator}"])
            sql += cls._get_sql(sq_data, sqldb)

            for _ in data["select"]["union"]["union_type"]:
                union_type = data["select"]["union"]["union_type"][iterator - 1]
                sq_data = deepcopy(data[f"sq{iterator+1}"])
                right_table = cls._get_sql(sq_data, sqldb)

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
                left_table = cls._get_sql(sq_data, sqldb)
                sql += f" FROM ({left_table}) sq{iterator}"

                for _ in data["select"]["join"]["join_type"]:
                    join_type = data["select"]["join"]["join_type"][iterator - 1]
                    sq_data = deepcopy(data[f"sq{iterator+1}"])
                    right_table = cls._get_sql(sq_data, sqldb)
                    on = data["select"]["join"]["on"][iterator - 1]

                    sql += f" {join_type} ({right_table}) sq{iterator+1}"
                    if on not in {0, "0"}:
                        sql += f" ON {on}"
                    iterator += 1

            elif "table" not in data["select"] and "join" not in data["select"] and "sq" in data:
                sq_data = deepcopy(data["sq"])
                sq = cls._get_sql(sq_data, sqldb)
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

    def _build_column_strings(self):
        # quotes wrapping fields differ depending on database (NOT ONLY DIALECT)
        quote = self.source._quote
        if self.store == {}:
            return {}

        duplicates = self._get_duplicated_columns()
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

            pos = None
            # we take either position or expression - depends if field is in select
            if field in selected_fields and self.source._use_ordinal_position_notation:
                pos = str(selected_fields.index(field) + 1)

            if "group_by" in fields[field]:
                if fields[field]["group_by"].upper() == "GROUP":
                    group_dimensions.append(pos or expr)

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
                order_by.append(f"{pos or expr}{order}")

            if field in selected_fields:
                select_name = expr if expr == f"{quote}{alias}{quote}" else f'{expr} as "{alias}"'

                dtype = fields[field]["dtype"].upper()

                select_names.append(select_name)
                select_aliases.append(alias)
                types.append(dtype)

        sql_blocks = {
            "select_names": select_names,
            "select_aliases": select_aliases,
            "group_dimensions": group_dimensions,
            "group_values": group_values,
            "order_by": order_by,
            "types": types,
        }

        return sql_blocks

    @property
    @deprecation.deprecated(details="Use QFrame.source instead",)
    def sqldb(self):
        return self.source

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


def join(
    qframes: List[SQLDriver],
    join_type: Union[str, List[str]],
    on: Union[str, List[str]],
    unique_col: bool = True,
):
    """Join QFrames.

    Name of each field is a concat of
    `sq` + position of origin QFrame in `qframes` + `.` + alias in origin QFrame.
    If the fields have the same aliases
    in their origin QFrames they will have the same aliases in joined QFrame.

    By default the joined QFrame will contain all fields from the first QFrame
    and all fields from the other QFrames which are not in the first QFrame.
    This approach prevents duplicates. If you want to choose the columns, set `unique_col=False` and
    after performing join please remove fields with the same aliases or rename them.

    Parameters
    ----------
    qframes : list
        List of qframes
    join_type : str or list
        Join type or a list of join types.
    on : str or list
        List of on join conditions. In case of CROSS JOIN set the condition to 0.
        NOTE: Structure of the elements of this list is very specific.
        You always have to use prefix "sq{qframe_position}."
        if you want to refer to the column. Check examples.
    unique_col : bool, optional
        If True the joined QFrame will cotain all fields from the first QFrame
        and all fields from other QFrames which are not repeated. If False the
        joined QFrame will contain all fields from every QFrame, default True


    NOTE: Order of the elements in join_type and on list is important.

    Examples
    --------
    >>> from grizly import get_path, QFrame
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
            first_source = q.source
        else:
            assert first_source == q.source, f"QFrames have different datasources"
        q._create_sql_blocks()
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
                            "dtype": sq["fields"][field]["dtype"],
                            "as": alias,
                        }
                        break

    if isinstance(join_type, str):
        join_type = [join_type]
    if isinstance(on, (int, str)):
        on = [on]

    data["select"]["join"] = {"join_type": join_type, "on": on}

    out_qf = SQLDriver(store=data, source=qframes[0].source, logger=qframes[0].logger)

    out_qf.logger.debug("Data joined successfully.")
    if not unique_col:
        warnings.warn(
            "Please remove or rename duplicated columns."
            "Use your_qframe.show_duplicated_columns() to check duplicates.",
            UserWarning,
        )
    return out_qf


def union(
    qframes: List[SQLDriver],
    union_type: Union[Literal["UNION", "UNION ALL"], List[Literal["UNION", "UNION ALL"]]],
    union_by: Literal["position", "name"] = "position",
):
    """Union QFrames.

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
    assert len(qframes) == len(union_type) + 1, "Incorrect list size in parameter union_type."
    assert set(item.upper() for item in union_type) <= {
        "UNION",
        "UNION ALL",
    }, "Incorrect union type. Valid types: 'UNION', 'UNION ALL'."
    if union_by not in {"position", "name"}:
        raise ValueError("Invalid value for union_by. Valid values: 'position', 'name'.")

    data = {"select": {"fields": {}}}

    main_qf = qframes[0]
    main_qf._create_sql_blocks()
    data["sq1"] = deepcopy(main_qf.data)
    old_fields = deepcopy(main_qf.data["select"]["fields"])
    new_fields = deepcopy(main_qf.data["select"]["sql_blocks"]["select_aliases"])
    new_types = deepcopy(main_qf.data["select"]["sql_blocks"]["types"])
    qframes.pop(0)

    iterator = 2
    for qf in qframes:
        assert main_qf.source == qf.source, "QFrames have different datasources"
        qf._create_sql_blocks()
        qf_aliases = qf.data["select"]["sql_blocks"]["select_aliases"]
        assert len(new_fields) == len(
            qf_aliases
        ), f"Amount of fields in {iterator}. QFrame doesn't match amount of fields in 1. QFrame"

        if union_by == "name":
            field_diff_1 = set(new_fields) - set(qf_aliases)
            field_diff_2 = set(qf_aliases) - set(new_fields)
            assert field_diff_1 == set() and field_diff_2 == set(), (
                f"Aliases {field_diff_2} not found in 1. QFrame, aliases {field_diff_1} not found in {iterator}. QFrame.",
                "Use qf.rename() to rename fields or set option union_by='position'",
            )
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
            qf._create_sql_blocks()

        for new_field in new_fields:
            field_position = new_fields.index(new_field)
            new_type = new_types[field_position]
            qf_alias = qf.data["select"]["sql_blocks"]["select_aliases"][field_position]
            qf_type = qf.data["select"]["sql_blocks"]["types"][field_position]
            assert qf_type == new_type, (
                f"Types don't match. 1. QFrame alias: {new_field} type: {new_type}, {iterator}.",
                f"QFrame alias: {qf_alias} type: {qf_type}.",
            )

        data[f"sq{iterator}"] = deepcopy(qf.data)
        iterator += 1

    for field in old_fields:
        if "select" in old_fields[field] and old_fields[field]["select"] == 0:
            continue
        else:
            if "as" in old_fields[field] and old_fields[field]["as"] != "":
                alias = old_fields[field].get("as") or field
            else:
                alias = field

            data["select"]["fields"][alias] = {"dtype": old_fields[field]["dtype"]}

    data["select"]["union"] = {"union_type": union_type}

    out_qf = SQLDriver(store=data, source=qframes[0].source, logger=qframes[0].logger)
    out_qf.logger.debug("Data unioned successfully.")

    return out_qf
