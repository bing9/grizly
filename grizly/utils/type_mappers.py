import re


def mysql_to_postgres_type(dtype):
    dtype = dtype.upper()

    dtypes = {
        "BIGINT$": "BIGINT",
        "BINARY(\d)$": "BYTEA",
        "BIT$": "BOOLEAN",
        "DATETIME$": "TIMESTAMP",
        "DATE$": "DATE",
        "DOUBLE$": "DOUBLE PRECISION",
        "FLOAT$": "REAL",
        "INTEGER$": "INTEGER",
        "MEDIUMINT$": "INTEGER",
        "SMALLINT$": "SMALLINT",
        "TINYBLOB$": "BYTEA",
        "BLOB$": "BYTEA",
        "MEDIUMBLOB$": "BYTEA",
        "LONGBLOB$": "BYTEA",
        "TINYINT$": "SMALLINT",
        "TINYTEXT$": "TEXT",
        "TEXT$": "TEXT",
        "MEDIUMTEXT$": "TEXT",
        "LONGTEXT$": "TEXT",
        "TIMESTAMP$": "TIMESTAMP",
        "TIME$": "TIME",
        "VARBINARY(\d)$": "BYTEA",
        "VARBINARY(MAX)$": "BYTEA",
        "VARCHAR(MAX)$": "TEXT",
        "BIGINT AUTO_INCREMENT$": "BIGSERIAL",
        "INTEGER AUTO_INCREMENT$": "SERIAL",
        "SMALLINT AUTO_INCREMENT$": "SMALLSERIAL",
        "TINYINT AUTO_INCREMENT$": "SMALLSERIAL",
        "BIGINT UNSIGNED$": "NUMERIC(20)",
        "INT$": "INT",
        "INT UNSIGNED$": "BIGINT",
        "MEDIUMINT UNSIGNED$": "INTEGER",
        "SMALLINT UNSIGNED$": "INTEGER",
        "TINYINT UNSIGNED$": "INTEGER",
    }

    import re

    for pyarrow_dtype in dtypes:
        if re.search(pyarrow_dtype, dtype):
            return dtypes[pyarrow_dtype]
    else:
        return dtype


def check_if_valid_type(type: str):
    """Checks if given type is valid in Redshift.

    Parameters
    ----------
    type : str
        Input type

    Returns
    -------
    bool
        True if type is valid, False if not
    """
    valid_types = [
        "SMALLINT",
        "INT2",
        "INTEGER",
        "INT",
        "INT4",
        "BIGINT",
        "INT8",
        "DECIMAL",
        "NUMERIC",
        "REAL",
        "FLOAT4",
        "DOUBLE PRECISION",
        "FLOAT8",
        "FLOAT",
        "BOOLEAN",
        "BOOL",
        "CHAR",
        "CHARACTER",
        "NCHAR",
        "BPCHAR",
        "VARCHAR",
        "CHARACTER VARYING",
        "NVARCHAR",
        "TEXT",
        "DATE",
        "TIMESTAMP",
        "TIMESTAMP WITHOUT TIME ZONE",
        "TIMESTAMPTZ",
        "TIMESTAMP WITH TIME ZONE",
    ]

    for valid_type in valid_types:
        if type.upper().startswith(valid_type):
            return True
    return False


def pyarrow_to_rds_type(dtype):
    dtypes = {
        "bool": "BOOL",
        "int8": "SMALLINT",
        "int16": "INT2",
        "int32": "INT4",
        "int64": "INT8",
        "uint8": "INT",
        "uint16": "INT",
        "uint32": "INT",
        "uint64": "INT",
        "float32": "FLOAT4",
        "float64": "FLOAT8",
        "double": "FLOAT8",
        "null": "FLOAT8",
        "date": "DATE",
        "string": "VARCHAR(500)",
        "timestamp.*\s*": "TIMESTAMP",
        "datetime.*\s*": "TIMESTAMP",
    }

    for pyarrow_dtype in dtypes:
        if re.search(pyarrow_dtype, dtype):
            return dtypes[pyarrow_dtype]
    else:
        return "VARCHAR(500)"
