from .sql import SQLDriver
import datetime


class SFDCDriver(SQLDriver):
    def __init__(self, source, table=None):
        super().__init__(source=source, table=table)

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

    def _cast_column_values(self, column_number, column_dtype, records):
        """Fix columns with mixed dtypes"""
        if "string" in column_dtype:
            column_values = [str(line[column_number]) for line in records]
        elif "float" in column_dtype:
            column_values = [float(line[column_number]) for line in records]
        elif "date" in column_dtype and type(records[0][column_number]) == str:
            column_values = [
                None
                if not line[column_number]
                else datetime.datetime.strptime(line[column_number], "%Y-%m-%d")
                for line in records
            ]
        else:
            column_values = [line[column_number] for line in records]
        return column_values

    def to_records(self):
        self._validate_fields()
        query = self.get_sql()
        sf_table = getattr(self.source.con, self.data["table"])
        response = sf_table.query(query)
        records = []
        for i in range(len(response)):
            response[i].pop("attributes")
            records.append(tuple(response[i].values()))
        return records

    def to_dict(self):
        _dict = {}
        records = self.to_records()
        columns = self.columns
        types = self.dtypes
        for i, column in enumerate(columns):
            dtype_mapped = self._to_pyarrow_dtype(dtype=types[i])
            column_values = self._cast_column_values(
                column_number=i, column_dtype=column_dtype, records=records
            )
            _dict[self.data["select"]["fields"][column]["as"]] = column_values
        return _dict