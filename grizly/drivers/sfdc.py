from __future__ import annotations
from datetime import date, datetime
import re
from sqlite3.dbapi2 import NotSupportedError
from typing import Any, List, Union

import numpy as np
import pyarrow as pa
from pyarrow.types import (
    is_date32,
    is_float32,
    is_float64,
    is_floating,
    is_string,
    is_temporal,
    is_timestamp,
)

from ..types import SFDB
from ..utils.type_mappers import sfdc_to_pyarrow
from .sql import SQLDriver


class SFDCDriver(SQLDriver):
    def __init__(self, batch_size: int = 20000, **kwargs):
        super().__init__(**kwargs)
        self.batch_size = batch_size

    def to_records(self) -> List[tuple]:
        self._validate_fields()
        records = self.source._fetch_records(self.get_sql(), self.store["select"]["table"])
        records_casted = self._cast_records(records)
        return records_casted

    # def rename(self, *args, **kwargs):
    #     raise NotSupportedError("Field aliases are not supported in SOSQL")

    def _validate_fields(self):
        """Check if requested fields are in SF table
        and if can be pulled (we can't pull compound fields)
        """
        compound_fields = self._get_compound_fields()
        if compound_fields:
            raise ValueError(
                "Compound fields are unsupported. Please remove the following fields:"
                f"{compound_fields}"
            )

    def _get_compound_fields(self) -> List[str]:
        fields_and_types = dict(zip(self.fields, self.types))
        fields_and_types_cleaned = {
            field: _type.split("(")[0] for field, _type in fields_and_types.items()
        }
        compound_types = ("address", "location")
        compound_fields = [
            field
            for field in fields_and_types_cleaned
            if fields_and_types_cleaned[field].lower() in compound_types
        ]
        return compound_fields

    def remove_compound_fields(self) -> SFDCDriver:
        for field in self._get_compound_fields():
            self.remove(field)
        return self

    def _cast_records(self, records: List[tuple]) -> List[tuple]:
        dtypes = self.dtypes  # costly property, so we only execute it once here
        # ------
        # to avoid searching outer scope gazillion times
        sf_to_pyarrow = sfdc_to_pyarrow
        cast = self._cast
        # ------
        casted = []
        for record in records:
            record_casted = []
            for i, val in enumerate(record):
                col_dtype = dtypes[i]
                pyarrow_dtype = sf_to_pyarrow(col_dtype)
                try:
                    val_casted = cast(val, dtype=pyarrow_dtype)
                except (AssertionError, NotImplementedError):
                    msg = f"Column {self.columns[i]} seems to be in an unsupported format"
                    self.logger.exception(msg)
                    raise
                record_casted.append(val_casted)
            casted.append(tuple(record_casted))
        return casted

    def _cast(self, val: Any, dtype: pa.DataType) -> Any:
        """Fix columns with mixed/serialized dtypes"""

        if not val:
            return None

        if is_string(dtype):
            casted = str(val)
        elif is_floating(dtype):
            casted = self._cast_float(val, dtype)
        elif is_temporal(dtype):
            casted = self._cast_temporal(val, dtype)
        else:
            casted = val
        return casted

    @staticmethod
    def _cast_float(val: Any, dtype: pa.DataType) -> Union[np.float32, np.float64]:
        if is_float32(dtype):
            casted = np.float32(val)
        elif is_float64(dtype):
            casted = np.float64(val)
        else:
            raise NotImplementedError
        return casted

    @staticmethod
    def _cast_temporal(val: Union[str, int], dtype: pa.DataType) -> Union[date, datetime]:
        if is_date32(dtype):  # and type(val) == str:
            casted = datetime.strptime(val, "%Y-%m-%d").date()
        elif is_timestamp(dtype):
            if type(val) == str:
                casted = datetime.strptime(val, "%Y-%m-%dT%H:%M:%S.%f%z")
            elif type(val) == int:
                # check if it's UTC
                assert len(str(val)) == 13, "Unrecognized timestamp format"
                tz_str = str(val)[-3:]
                utc_tz_str = "000"
                if tz_str == utc_tz_str:
                    casted = datetime.fromtimestamp(val / 1000)
                else:
                    # should convert to UTC, but hopefully we don't have to bother
                    raise NotImplementedError("Casting non-UTC timestamps is not yet supported.")
            else:
                raise ValueError("A serialized date must be a string or integer")
        else:
            raise NotImplementedError(
                "Currently, only casting to date32 and timestamp is supported"
            )
        return casted

    def describe(self):
        return dict(zip(self.columns, self.types))

    def select(self):
        raise NotImplementedError("Subquerying is not possible in SOSQL")

    def groupby(self, fields: Union[List[str], str] = None) -> SFDCDriver:
        """Adds GROUP BY statement.

        Parameters
        ----------
        fields : list or string
            Field or list of fields, if None then all fields are grouped. Fields must be groupable.

        Examples
        --------
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
        if isinstance(fields, str):
            fields = [fields]
        table = self.data["select"]["table"]
        fields_to_validate = fields or self.get_fields()
        self._validate_groupable(table=table, fields=fields_to_validate)
        super().groupby(fields=fields)
        return self

    def _validate_groupable(self, table: str, fields: List[str]):
        fields_info = self.source.table(table).sf_table.describe()["fields"]
        non_groupable = [field["name"] for field in fields_info if not field["groupable"]]
        invalid_fields = [field for field in fields if field in non_groupable]
        if invalid_fields:
            raise ValueError(
                f"Ungroupable fields found: {invalid_fields}. Please remove them from your query."
            )

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

            alias = field

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
                select_name = expr

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
    def nrows(self):
        # query = f"SELECT COUNT() FROM ({self.get_sql()}) sq"
        # records = self.source._fetch_records(query, table=self.store["select"]["table"])
        # if records:
        #     nrows = records[0][0]
        # else:
        #     nrows = 0
        # return nrows
        return 0
