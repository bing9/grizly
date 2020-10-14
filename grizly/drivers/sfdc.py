import datetime
from logging import Logger
from typing import List, Union

from ..sources.rdbms.sfdc import sfdb
from ..types import SFDB
from ..utils.type_mappers import sfdc_to_pyarrow
from .sql import SQLDriver


class SFDCDriver(SQLDriver):
    def __init__(self, source: SFDB = sfdb, table: str = None, logger: Logger = None):
        super().__init__(source=source, table=table, logger=logger)

    def _cast_records(self, records):
        casted = []
        for record in records:
            record_casted = []
            for i, val in enumerate(record):
                col_dtype = self.dtypes[i]
                val_casted = self._cast(val, dtype=col_dtype)
                record_casted.append(val_casted)
            casted.append(tuple(record_casted))
        return casted

    def to_records(self):
        self._validate_fields()
        query = self.get_sql()
        response = self.source.con.query(query)
        records_raw = response["records"]
        records = self._sfdc_records_to_records(records_raw)
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
        fields_and_types = dict(zip(self.fields, self.types))
        compound_types = ("address", "location")
        compound_fields = [
            field for field in fields_and_types if fields_and_types[field] in compound_types
        ]
        if compound_fields:
            raise ValueError(
                "Compound fields are unsupported. Please remove the following fields:"
                f"{compound_fields}"
            )

    def _cast(self, val, dtype="auto"):
        """Fix columns with mixed dtypes"""

        if not val:
            return None

        dtype_mapped = sfdc_to_pyarrow(dtype)

        dtype_str = str(dtype_mapped)
        if "string" in dtype_str:
            casted = str(val)
        elif "float" in dtype_str:
            casted = float(val)
        elif "date32" in dtype_str and type(val) == str:
            casted = datetime.datetime.strptime(val, "%Y-%m-%d")
        elif "timestamp" in dtype_str and type(val) == str:
            casted = datetime.datetime.strptime(val, "%Y-%m-%dT%H:%M:%S.%f%z")
        else:
            return val
        return casted

    @staticmethod
    def _sfdc_records_to_records(sfdc_records):
        """Convert weird SFDC response to records"""
        records = []
        for i in range(len(sfdc_records)):
            sfdc_records[i].pop("attributes")
            records.append(tuple(sfdc_records[i].values()))
        return records

    def _validate_groupable(self, table: str, fields: List[str]):
        fields_info = self.source.table(table).sf_table.describe()["fields"]
        non_groupable = [field["name"] for field in fields_info if not field["groupable"]]
        invalid_fields = [field for field in fields if field in non_groupable]
        if invalid_fields:
            raise ValueError(
                f"Ungroupable fields found: {invalid_fields}. Please remove them from your query."
            )

    def select(self):
        raise NotImplementedError("Subquerying is not possible in SOSQL")

    def groupby(self, fields: Union[List[str], str] = None):
        """Adds GROUP BY statement.

        Parameters
        ----------
        fields : list or string
            List of fields or a field, if None then all fields are grouped. Fields must be groupable.

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
        table = self.data["select"]["table"]
        fields_to_validate = fields or self.get_fields()
        self._validate_groupable(table=table, fields=fields_to_validate)
        super().groupby(fields=fields)
        return self
