from __future__ import annotations

import datetime
from logging import Logger
from typing import List, Union, Dict, Any

from ..sources.rdbms.sfdc import sfdb
from ..types import SFDB
from ..utils.type_mappers import sfdc_to_pyarrow
from .sql import SQLDriver


class SFDCDriver(SQLDriver):
    def __init__(self, source: SFDB = sfdb, table: str = None, logger: Logger = None):
        super().__init__(source=source, table=table, logger=logger)

    def to_records(self) -> List[tuple]:
        self._validate_fields()
        query = self.get_sql()
        table = getattr(self.source.con.bulk, self.table)
        response = table.query(query)
        # records_raw = response["records"]  # this is for non-bulk API
        records = self._sfdc_records_to_records(response)
        # records_casted = [
        #     tuple(self._cast(val, dtype=self.dtypes[i]) for i, val in enumerate(record))
        #     for record in records
        # ]
        records_casted = self._cast_records(records)
        return records_casted

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
            if fields_and_types_cleaned[field] in compound_types
        ]
        return compound_fields

    def _remove_compound_fields(self) -> SFDCDriver:
        for field in self._get_compound_fields():
            self.remove(field)
        return self

    @staticmethod
    def _sfdc_records_to_records(sfdc_records: List[Dict[str, Any]]) -> List[tuple]:
        """Convert weird SFDC response to records"""
        records = []
        for i in range(len(sfdc_records)):
            sfdc_records[i].pop("attributes")
            records.append(tuple(sfdc_records[i].values()))
        return records

    def _cast_records(self, records: List[tuple]) -> List[tuple]:
        casted = []
        for record in records:
            record_casted = []
            for i, val in enumerate(record):
                col_dtype = self.dtypes[i]
                try:
                    val_casted = self._cast(val, dtype=col_dtype)
                except (AssertionError, NotImplementedError):
                    msg = f"Column {self.columns[i]} seems to be in an unsupported format"
                    self.logger.exception(msg)
                    raise
                record_casted.append(val_casted)
            casted.append(tuple(record_casted))
        return casted

    def _cast(self, val: Any, dtype: str) -> Any:
        """Fix columns with mixed/serialized dtypes"""

        if not val:
            return None

        dtype_mapped = sfdc_to_pyarrow(dtype)

        dtype_str = str(dtype_mapped)
        if "string" in dtype_str:
            casted = str(val)
        elif "float" in dtype_str or "double" in dtype_str:
            # self.logger.info(f"{val}, {dtype_str}")
            casted = float(val)
        elif "date32" in dtype_str and type(val) == str:
            casted = datetime.datetime.strptime(val, "%Y-%m-%d")
        # TODO: put below logic in _cast_datetime()
        elif "timestamp" in dtype_str and type(val) == str:
            casted = datetime.datetime.strptime(val, "%Y-%m-%dT%H:%M:%S.%f%z")
        elif "timestamp" in dtype_str and type(val) == int:
            # check if it's UTC
            assert len(str(val)) == 13, "Unrecognized timestamp format"
            tz_str = str(val)[-3:]
            utc_tz_str = "000"
            if tz_str == utc_tz_str:
                casted = datetime.datetime.fromtimestamp(val / 1000)
            else:
                # should convert to UTC, but hopefully we don't have to bother
                raise NotImplementedError("Casting non-UTC timestamps is not yet supported.")
        else:
            return val
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
