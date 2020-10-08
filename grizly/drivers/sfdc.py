from .sql import SQLDriver
import datetime
from ..utils.type_mappers import sfdc_to_pyarrow_dtype


class SFDCDriver(SQLDriver):
    def __init__(self, source, table=None):
        super().__init__(source=source, table=table)

    def to_records(self):
        self._validate_fields()
        query = self.get_sql()
        response = self.source.con.query(query)
        records_raw = response["records"]
        records = self._sfdc_records_to_records(records_raw)
        columns, types = self.columns, self.dtypes
        _dict = {}
        for i, column in enumerate(columns):
            col_dtype = types[i]
            col_dtype_mapped = sfdc_to_pyarrow_dtype(col_dtype)
            for record in records:
                record = [self._cast(val, dtype=col_dtype_mapped) for val in record]
            col_dtype_mapped = sfdc_to_pyarrow_dtype(col_dtype)
            col_values_casted = self._cast(column, col_dtype_mapped)
            _dict[column] = col_values_casted
        return records

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

    @staticmethod
    def _cast(val, dtype):
        """Fix columns with mixed dtypes"""

        if not val:
            return None

        dtype_str = str(dtype)
        if "string" in dtype_str:
            val = str(val)
        elif "float" in dtype_str:
            val = float(val)
        elif "date" in dtype_str and type(val) == str:
            val = datetime.datetime.strptime(val, "%Y-%m-%d")
        else:
            return val
        return val

    @staticmethod
    def _sfdc_records_to_records(sfdc_records):
        """Convert weird SFDC response to records"""
        records = []
        for i in range(len(sfdc_records)):
            sfdc_records[i].pop("attributes")
            records.append(tuple(sfdc_records[i].values()))
        return records

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
        # assert (
        #     "union" not in self.store["select"]
        # ), "You can't group by inside union. Use select() method first."

        _validate_groupable(fields)
        output = super().get_tables(schema=schema, base_table=base_table, view=view)
        return self
