import re
import pyarrow as pa
import datetime
import numpy as np


def _map_type(mapping, dtype, default=None, method="advanced"):
    if not default:
        default = dtype
    if method == "simple":
        return mapping.get(dtype, default)
    elif method == "intermediate":
        dtype_no_length = dtype.split("(")[0]
        return mapping.get(dtype_no_length, default)
    else:
        if isinstance(dtype, str):
            # fuzzy search
            variable_field = re.compile("^([a-zA-Z])*(\(\d+\))")
            match = variable_field.search(dtype)
            field_is_var_length = bool(match)
            if field_is_var_length:
                length = match.group(2).strip("(").strip(")")
                for unmapped in mapping:
                    if re.search(unmapped, dtype):
                        if isinstance(mapping[unmapped], str):
                            return mapping[unmapped] + f"({length})"
                        else:
                            return mapping[unmapped]
            for source_dtype in mapping:
                if re.search(source_dtype, dtype):
                    return mapping[source_dtype]
        return mapping.get(dtype, default)  # exact search


def mysql_to_postgresql(dtype):
    dtype = dtype.upper()
    mapping = {
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
    return _map_type(mapping, dtype)


def pyarrow_to_postgresql(dtype):
    mapping = {
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
    return _map_type(mapping, dtype, default="VARCHAR(500)")


def pyarrow_to_rds(dtype):
    return pyarrow_to_postgresql(dtype=dtype)


def postgresql_to_pyarrow(dtype):
    mapping = {
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
    return _map_type(mapping, dtype, default=pa.string())


def rds_to_pyarrow(dtype):
    return postgresql_to_pyarrow(dtype=dtype)


def mysql_to_pyarrow(dtype):
    # TODO: fix below mapper
    mapping = {
        "BOOL": pa.bool_(),
        "BIT": pa.bool_(),
        "INT": pa.int32(),
        "INTEGER": pa.int32(),
        "MEDIUMINT": pa.int32(),
        "SMALLINT": pa.int8(),
        "TINYINT": pa.int8(),
        "BIGINT": pa.int64(),
        "INT2": pa.int8(),
        "INT4": pa.int8(),
        "INT8": pa.int8(),
        "NUMERIC": pa.float64(),
        "DECIMAL": pa.float64(),
        "FLOAT4": pa.float32(),
        "FLOAT8": pa.float64(),
        "DOUBLE": pa.float64(),
        "FLOAT": pa.float32(),
        "NULL": pa.null(),
        "DATE": pa.date64(),
        "VARCHAR": pa.string(),
        "NVARCHAR": pa.string(),
        "CHARACTER VARYING": pa.string(),
        "TEXT": pa.string(),
        "CHAR": pa.string(),
        "CHARACTER": pa.string(),
        "DATETIME": pa.date64(),
        "TIMESTAMP": pa.date64(),
        "TIMESTAMP WITHOUT TIME ZONE": pa.date64(),
        "TIMESTAMPTZ": pa.date64(),
        "TIMESTAMP WITH TIME ZONE": pa.date64(),
        "GEOMETRY": None,
    }
    return _map_type(mapping, dtype, default=pa.string())


def sfdc_to_pyarrow(dtype: str):
    mapping = {
        "address": pa.string(),
        "anytype": pa.string(),
        "base64": pa.string(),
        "boolean": pa.bool_(),
        "combobox": pa.string(),
        "currency": pa.float64(),
        "datacategorygroupreference": pa.string(),
        "datetime": pa.timestamp("ms", tz="utc"),
        "date": pa.date32(),
        "double": pa.float64(),
        "email": pa.string(),
        "encryptedstring": pa.string(),
        "id": pa.string(),
        "int8": pa.int8(),
        "int": pa.int32(),
        "multipicklist": pa.string(),
        "percent": pa.float32(),
        "phone": pa.string(),
        "picklist": pa.string(),
        "reference": pa.string(),
        "string": pa.string(),
        "textarea": pa.string(),
        "time": pa.time64("us"),
        "url": pa.string(),
    }
    return _map_type(mapping, dtype, default=pa.string(), method="intermediate")


def sfdc_to_sqlalchemy(dtype):
    mapping = {
        "address": "VARCHAR",
        "anytype": "VARCHAR",
        "base64": "VARCHAR",
        "boolean": "BOOLEAN",
        "combobox": "VARCHAR",
        "currency": "DOUBLE PRECISION",
        "datacategorygroupreference": "VARCHAR",
        "datetime": "DATETIME",
        "date": "DATE",
        "double": "DOUBLE PRECISION",
        "email": "VARCHAR(128)",
        "encryptedstring": "VARCHAR",
        "id": "VARCHAR(18)",
        "int": "INT",
        "multipicklist": "VARCHAR",
        "percent": "FLOAT4",
        "phone": "VARCHAR(40)",
        "picklist": "VARCHAR",
        "reference": "VARCHAR(18)",
        "string": "VARCHAR",
        "textarea": "VARCHAR",
        "time": "DATETIME",
        "url": "VARCHAR(1024)",
    }
    return _map_type(mapping, dtype, default="VARCHAR(255)")


def sfdc_to_python(dtype):
    mapping = {
        "address": str,
        "anytype": str,
        "base64": str,
        "boolean": bool,
        "combobox": str,
        "currency": np.float64,
        "datacategorygroupreference": str,
        "datetime": datetime.datetime,
        "date": datetime.date,
        "double": np.float64,
        "email": str,
        "encryptedstring": str,
        "id": str,
        "int": int,
        "multipicklist": str,
        "percent": np.float32,
        "phone": str,
        "picklist": str,
        "reference": str,
        "string": str,
        "textarea": str,
        "time": datetime.datetime,
        "url": str,
    }
    return _map_type(mapping, dtype, default=str)


def postgresql_to_python(dtype):
    # TODO: fix below mapper
    mapping = {
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
    return _map_type(mapping, dtype, default=str)


def sql_to_python(dtype):
    return postgresql_to_python(dtype=dtype)


def mysql_to_python(dtype):
    # TODO: fix below mapper
    mapping = {
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
        "DOUBLE": float,
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
    return _map_type(mapping, dtype, default=str)


def python_to_sql(dtype):
    mapping = {str: "VARCHAR(50)", int: "INTEGER", float: "FLOAT8"}
    return _map_type(mapping, dtype, default="VARCHAR(255)")


valid_redshift_types = [
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
