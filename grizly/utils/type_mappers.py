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

def rds_to_pyarrow_type(dtype):
    dtypes = {
        "BOOL": pa.bool_(),
        "BOOLEAN": pa.bool_(),
        "INT": pa.int32(),
        "INTEGER": pa.int32(),
        "SMALLINT": pa.int8(),
        "BIGINT": pa.int64(),
        "INT2": pa.int8(),
        "INT4": pa.int8(),
        "INT8": pa.int8(),
        "NUMERIC": pa.float64(),
        "DECIMAL": pa.float64(),
        "FLOAT4": pa.float32(),
        "FLOAT8": pa.float64(),
        "DOUBLE PRECISION": pa.float64(),
        "REAL": pa.float32(),
        "NULL": pa.null(),
        "DATE": pa.date64(),
        "VARCHAR": pa.string(),
        "NVARCHAR": pa.string(),
        "CHARACTER VARYING": pa.string(),
        "TEXT": pa.string(),
        "CHAR": pa.string(),
        "CHARACTER": pa.string(),
        "TIMESTAMP": pa.date64(),
        "TIMESTAMP WITHOUT TIME ZONE": pa.date64(),
        "TIMESTAMPTZ": pa.date64(),
        "TIMESTAMP WITH TIME ZONE": pa.date64(),
        "GEOMETRY": None,
    }

    for redshift_dtype in dtypes:
        if re.search(redshift_dtype, dtype):
            return dtypes[redshift_dtype]
    else:
        return pa.string()

def sfdc_to_sqlalchemy_dtype(sfdc_dtype):
    """Get SQLAlchemy equivalent of the given SFDC data type.

    Parameters
    ----------
    sfdc_dtype : str
        SFDC data type.

    Returns
    ----------
    sqlalchemy_dtype : str
        The string representing a SQLAlchemy data type.
    """

    sqlalchemy_dtypes = {
        "address": "NVARCHAR",
        "anytype": "NVARCHAR",
        "base64": "NVARCHAR",
        "boolean": "BOOLEAN",
        "combobox": "NVARCHAR",
        "currency": "NUMERIC(precision=14)",
        "datacategorygroupreference": "NVARCHAR",
        "date": "DATE",
        "datetime": "DATETIME",
        "double": "NUMERIC",
        "email": "NVARCHAR",
        "encryptedstring": "NVARCHAR",
        "id": "NVARCHAR",
        "int": "INT",
        "multipicklist": "NVARCHAR",
        "percent": "NUMERIC(precision=6)",
        "phone": "NVARCHAR",
        "picklist": "NVARCHAR",
        "reference": "NVARCHAR",
        "string": "NVARCHAR",
        "textarea": "NVARCHAR",
        "time": "DATETIME",
        "url": "NVARCHAR",
    }
    sqlalchemy_dtype = sqlalchemy_dtypes[sfdc_dtype]
    return sqlalchemy_dtype


def sql_to_python_dtype(dtype):
    dtypes = {
        "BOOL": bool,
        "BOOLEAN": bool,
        "INT": int,
        "INTEGER": int,
        "SMALLINT": int,
        "BIGINT": int,
        "INT2": int,
        "INT4": int,
        "INT8": int,
        "NUMERIC": float,
        "DECIMAL": float,
        "FLOAT4": float,
        "FLOAT8": float,
        "DOUBLE PRECISION": float,
        "REAL": float,
        "NULL": None,
        "DATE": datetime.date,
        "VARCHAR": str,
        "NVARCHAR": str,
        "CHARACTER VARYING": str,
        "TEXT": str,
        "CHAR": str,
        "CHARACTER": str,
        "TIMESTAMP": datetime.datetime,
        "TIMESTAMP WITHOUT TIME ZONE": datetime.datetime,
        "TIMESTAMPTZ": datetime.datetime,
        "TIMESTAMP WITH TIME ZONE": datetime.datetime,
        "GEOMETRY": None,
    }

    for sql_dtype in dtypes:
        if re.search(sql_dtype, dtype):
            return dtypes[sql_dtype]
    else:
        return str