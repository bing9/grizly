from typing import List, Literal

from ...utils.type_mappers import postgresql_to_pyarrow, postgresql_to_python
from .base import RDBMSBase


class Redshift(RDBMSBase):
    dialect = "postgresql"

    def create_table(
        self,
        table: str,
        columns: list,
        types: list,
        schema: str = None,
        if_exists: str = "skip",
        table_type: Literal["base", "external"] = "base",
        **kwargs,
    ):
        """Extends parent class with external tables"""
        if table_type == "external":
            if not (("bucket" in kwargs and "s3_key" in kwargs) or "s3_url" in kwargs):
                msg = "'bucket' and 's3_key' or 's3_url' parameters are required"
                raise ValueError(msg)
            bucket = kwargs.get("bucket")
            s3_key = kwargs.get("s3_key")
            s3_url = kwargs.get("s3_url")
            self._create_external_table(
                table=table,
                columns=columns,
                types=types,
                schema=schema,
                if_exists=if_exists,
                bucket=bucket,
                s3_key=s3_key,
                s3_url=s3_url,
            )
        else:
            super().create_table(
                table=table, columns=columns, types=types, schema=schema, if_exists=if_exists
            )

        return self

    def _create_external_table(
        self,
        table: str,
        columns: List[str],
        types: List[str],
        bucket: str = None,
        s3_key: str = None,
        s3_url: str = None,
        schema: str = None,
        if_exists: str = "skip",
    ):
        """Creates an external table"""
        valid_if_exists = ("fail", "skip", "drop")
        if if_exists not in valid_if_exists:
            raise ValueError(
                f"'{if_exists}' is not valid for if_exists. Valid values: {valid_if_exists}"
            )

        full_table_name = schema + "." + table if schema else table
        s3_url = s3_url or f"s3://{bucket}/{s3_key}"

        if self.check_if_exists(table=table, schema=schema):
            if if_exists == "fail":
                raise ValueError(
                    f"Table {full_table_name} already exists and if_exists is set to 'fail'."
                )
            elif if_exists == "skip":
                self.logger.info(
                    f"Table {full_table_name} already exists and if_exists is set to 'skip'."
                )
                return self
            elif if_exists == "drop":
                self.drop_table(table=table, schema=schema)

        columns_and_dtypes = ", \n".join([col + " " + dtype for col, dtype in zip(columns, types)])
        sql = f"""
        CREATE EXTERNAL TABLE {full_table_name} (
        {columns_and_dtypes}
        )
        ROW FORMAT SERDE
        'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
        STORED AS INPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
        OUTPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
        location '{s3_url}';
        """
        self._run_query(sql, autocommit=True)
        self.logger.info(f"Table {full_table_name} has been successfully created.")

        return self

    def get_tables(self, schema=None, base_table=True, view=True, external_table=True):
        """Extends parent class with external tables"""
        output = super().get_tables(schema=schema, base_table=base_table, view=view)

        if external_table:
            output += self._get_external_tables(schema=schema)

        return output

    def _get_external_tables(self, schema=None):
        where = f"\nWHERE schemaname = '{schema}'\n" if schema else ""

        sql = f"""
            SELECT schemaname, tablename
            FROM svv_external_tables{where}
            GROUP BY 1, 2
            """
        records = self._fetch_records(sql)

        return records

    def get_columns(self, table, schema=None, column_types=False, columns=None):
        """Extends parent class with external tables"""
        if (schema, table) in self._get_external_tables(schema=schema):
            return self._get_external_columns(
                schema=schema, table=table, column_types=column_types, columns=columns
            )
        else:
            return super().get_columns(
                table=table, schema=schema, column_types=column_types, columns=columns
            )

    def _get_external_columns(
        self, table, schema: str = None, column_types: bool = False, columns: list = None
    ):
        where = f" AND schemaname = '{schema}'\n" if schema else ""

        sql = f"""
            SELECT columnnum,
                columnname,
                external_type
            FROM SVV_EXTERNAL_COLUMNS
            WHERE tablename = '{table}'{where}
            ORDER BY 1
            """
        records = self._fetch_records(sql)

        if columns is not None:
            col_names = [col for _, col, _ in records if col in columns]
            col_types = [typ for _, col, typ in records if col in columns]
        else:
            col_names = [col for _, col, _ in records]
            col_types = [typ for _, _, typ in records]

        return (col_names, col_types) if column_types else col_names

    @staticmethod
    def map_types(dtypes: List[str], to: str = None):
        if to == "python":
            return [postgresql_to_python(dtype) for dtype in dtypes]
        elif to == "pyarrow":
            return [postgresql_to_pyarrow(dtype) for dtype in dtypes]
        else:
            raise NotImplementedError
