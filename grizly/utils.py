import os
import json
from sqlalchemy import create_engine
import pandas as pd
from sqlalchemy.pool import NullPool
from simple_salesforce import Salesforce
from simple_salesforce.login import SalesforceAuthenticationFailed

class Config(dict):
    def __init__(self):
        dict.__init__(self)

    def from_dict(self, d):
        for key in d:
            self[key] = d[key]


def read_config():
    try:
        json_path = os.path.join(os.environ['USERPROFILE'], '.grizly', 'etl_config.json')
        with open(json_path, 'r') as f:
            config = json.load(f)
    except KeyError:
        config = "Error with UserProfile"
    return config


config = read_config()
try:
    os.environ["HTTPS_PROXY"] = config["https"]
except TypeError:
    pass


def get_connection(db="denodo"):

    engine = create_engine(config[db])

    try:
        con = engine.connect().connection
    except:
        con = engine.connect().connection

    return con


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
        'anytype': "NVARCHAR",
        'base64': "NVARCHAR",
        'boolean': "BOOLEAN",
        'combobox': "NVARCHAR",
        'currency': "NUMERIC(precision=14)",
        'datacategorygroupreference': "NVARCHAR",
        'date': "DATE",
        'datetime': "DATETIME",
        'double': "NUMERIC",
        'email': "NVARCHAR",
        'encryptedstring': "NVARCHAR",
        'id': "NVARCHAR",
        'int': "INT",
        'multipicklist': "NVARCHAR",
        'percent': "NUMERIC(precision=6)",
        'phone': "NVARCHAR",
        'picklist': "NVARCHAR",
        'reference': "NVARCHAR",
        'string': "NVARCHAR",
        'textarea': "NVARCHAR",
        'time': "DATETIME",
        'url': "NVARCHAR"
    }
    sqlalchemy_dtype = sqlalchemy_dtypes[sfdc_dtype]
    return sqlalchemy_dtype


def get_sfdc_columns(table, columns=None, column_types=True):
    """Get column names (and optionally types) from a SFDC table.

    The columns are sent by SFDC in a messy format and the types are custom SFDC types,
    so they need to be manually converted to sql data types.

    Parameters
    ----------
    table : str
        Name of table.
    column_types : bool
        Whether to retrieve field types.

    Returns
    ----------
    List or Dict
    """

    sfdc_username = config["sfdc_username"]
    sfdc_pw = config["sfdc_password"]

    try:
        sf = Salesforce(password=sfdc_pw, username=sfdc_username, organizationId='00DE0000000Hkve')
    except SalesforceAuthenticationFailed:
        print("Could not log in to SFDC. Are you sure your password hasn't expired and your proxy is set up correctly?")
        raise SalesforceAuthenticationFailed
    field_descriptions = eval(f'sf.{table}.describe()["fields"]') # change to variable table
    types = {field["name"]: (field["type"], field["length"]) for field in field_descriptions}

    if columns:
        fields = columns
    else:
        fields = [field["name"] for field in field_descriptions]

    if column_types:
        dtypes = {}
        for field in fields:

            field_sfdc_type = types[field][0]
            field_len = types[field][1]
            field_sqlalchemy_type = sfdc_to_sqlalchemy_dtype(field_sfdc_type)
            if field_sqlalchemy_type == "NVARCHAR":
                field_sqlalchemy_type = f"{field_sqlalchemy_type}({field_len})"

            dtypes[field] = field_sqlalchemy_type
        return dtypes
    else:
        raise NotImplementedError("Retrieving columns only is currently not supported")


def get_denodo_columns(schema, table, column_types=False, columns=None, date_format="DATE", engine_str:str=None, db="denodo"):
    """Get column names (and optionally types) from Denodo view.

    Parameters
    ----------
    schema : str
        Name of schema.
    table : str
        Name of table.
    column_types : bool
        True means user wants to get also data types.
    date_format : str
        Denodo date format differs from those from other databases. User can choose which format is desired.
    engine_str : str
        Engine string
    db : str
        Key in etl_config.json (read if engine_str is None)
    """
    if column_types==False:
        sql = f"""
            SELECT column_name
            FROM get_view_columns()
            WHERE view_name = '{table}'
                AND database_name = '{schema}'
            """
    else:
        sql = f"""
            SELECT distinct column_name,  column_sql_type, column_size
            FROM get_view_columns()
            WHERE view_name = '{table}'
            AND database_name = '{schema}'
    """
    engine_str = engine_str or config.get(db) or "mssql+pyodbc://DenodoODBC"
    engine = create_engine(engine_str, encoding='utf8', poolclass=NullPool)

    try:
        con = engine.connect().connection
        cursor = con.cursor()
        cursor.execute(sql)
    except:
        con = engine.connect().connection
        cursor = con.cursor()
        cursor.execute(sql)

    col_names = []

    if column_types==False:
        while True:
            column = cursor.fetchone()
            if not column:
                break
            col_names.append(column[0])
        cursor.close()
        con.close()
        return col_names
    else:
        col_types = []
        while True:
            column = cursor.fetchone()
            if not column:
                break
            col_names.append(column[0])
            if column[1] in ( 'VARCHAR', 'NVARCHAR'):
                col_types.append(column[1]+'('+str(min(column[2],1000))+')')
            elif column[1] == 'DATE' :
                col_types.append(date_format)
            else:
                col_types.append(column[1])
        cursor.close()
        con.close()
        if columns:
            col_names_and_types = {col_name: col_type for col_name, col_type in zip(col_names, col_types) if col_name in columns}
            col_names = [col for col in col_names_and_types]
            col_types = [type for type in col_names_and_types.values()]
        return col_names, col_types


def get_redshift_columns(schema, table, column_types=False, engine_str:str=None, db="redshift"):
    """Get column names (and optionally types) from a Redshift table.

    Parameters
    ----------
    schema : str
        Name of schema.
    table : str
        Name of table.
    column_types : bool
        Whether to retrieve field types.
    date_format : str
        Denodo date format differs from those from other databases. User can choose which format is desired.
    engine_str : str
        Engine string
    db : str
        Key in etl_config.json (read if engine_str is None)
    """
    engine_str = engine_str or config.get(db) or "mssql+pyodbc://Redshift"
    engine = create_engine(engine_str, encoding='utf8', poolclass=NullPool)
    con = engine.connect().connection
    cursor = con.cursor()
    sql = f"""
        SELECT ordinal_position AS position, column_name, data_type,
        CASE WHEN character_maximum_length IS NOT NULL
        THEN character_maximum_length
        ELSE numeric_precision END AS max_length
        FROM information_schema.columns
        WHERE table_name = '{table}' AND table_schema = '{schema}'
        ORDER BY ordinal_position;
        """
    cursor.execute(sql)

    col_names = []

    if column_types:
        col_types = []
        while True:
            column = cursor.fetchone()
            if not column:
                break
            col_name = column[1]
            col_type = column[2]
            col_names.append(col_name)
            col_types.append(col_type)
        to_return = (col_names, col_types)
    else:
        while True:
            column = cursor.fetchone()
            if not column:
                break
            col_name = column[1]
            col_names.append(col_name)
        to_return = col_names

    cursor.close()
    con.close()

    return to_return


def get_columns(table, schema=None, column_types=False, date_format="DATE", db="denodo", columns=None, engine_str:str=None):
    """ Retrieves column names and optionally other table metadata """
    db = db.lower()
    if db == "denodo":
        return get_denodo_columns(schema=schema, table=table, column_types=column_types, date_format=date_format, columns=columns, engine_str=engine_str)
    elif db == "redshift":
        return get_redshift_columns(schema=schema, table=table, column_types=column_types, engine_str=engine_str)
    elif db == "sfdc":
        return get_sfdc_columns(table=table, column_types=column_types, columns=columns)
    else:
        raise NotImplementedError("This db is not yet supported")


def check_if_exists(table, schema='', redshift_str=None):
    """
    Checks if a table exists in Redshift.

    Parameters
    ----------
    redshift_str : str, optional
        Redshift engine string, if None then 'mssql+pyodbc://Redshift'
    """
    redshift_str = redshift_str if redshift_str else 'mssql+pyodbc://Redshift'

    engine = create_engine(redshift_str, encoding='utf8', poolclass=NullPool)
    if schema == '':
        sql_exists = "select * from information_schema.tables where table_name = '{}' ". format(table)
    else:
        sql_exists = "select * from information_schema.tables where table_schema = '{}' and table_name = '{}' ". format(schema, table)

    return not pd.read_sql_query(sql = sql_exists, con=engine).empty


def check_if_valid_type(type:str):
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
        'SMALLINT',
        'INT2',
        'INTEGER',
        'INT',
        'INT4',
        'BIGINT',
        'INT8',
        'DECIMAL',
        'NUMERIC',
        'REAL',
        'FLOAT4',
        'DOUBLE PRECISION',
        'FLOAT8',
        'FLOAT',
        'BOOLEAN',
        'BOOL',
        'CHAR',
        'CHARACTER',
        'NCHAR',
        'BPCHAR',
        'VARCHAR',
        'CHARACTER VARYING',
        'NVARCHAR',
        'TEXT',
        'DATE',
        'TIMESTAMP',
        'TIMESTAMP WITHOUT TIME ZONE',
        'TIMESTAMPTZ',
        'TIMESTAMP WITH TIME ZONE'
        ]

    for valid_type in valid_types:
        if type.upper().startswith(valid_type):
            return True
    return False


def delete_where(table, schema='', redshift_str=None, *argv):
    """
    Removes records from Redshift table which satisfy *argv.

    Parameters:
    ----------
    table : string
        Name of SQL table.
    schema : string, optional
        Specify the schema.
    redshift_str : str, optional
        Redshift engine string, if None then 'mssql+pyodbc://Redshift'

    Examples:
    --------
        >>> delete_where('test_table', schema='testing', "fiscal_year = '2019'")

        Will generate and execute query:
        "DELETE FROM testing.test WHERE fiscal_year = '2019'"


        >>> delete_where('test_table', schema='testing', "fiscal_year = '2017' OR fiscal_year = '2018'", "customer in ('Enel', 'Agip')")

        Will generate and execute two queries:
        "DELETE FROM testing.test WHERE fiscal_year = '2017' OR fiscal_year = '2018'"
        "DELETE FROM testing.test WHERE customer in ('Enel', 'Agip')"

    """
    table_name = f'{schema}.{table}' if schema else f'{table}'
    redshift_str = redshift_str if redshift_str else 'mssql+pyodbc://Redshift'

    if check_if_exists(table, schema):
        engine = create_engine(redshift_str, encoding='utf8', poolclass=NullPool)

        if argv is not None:
            for arg in argv:
                sql = f"DELETE FROM {table_name} WHERE {arg} "
                engine.execute(sql)
                print(f'Records from table {table_name} where {arg} has been removed successfully.')
    else:
        print(f"Table {table_name} doesn't exist.")


def copy_table(schema, copy_from, to, redshift_str=None):

    sql = f"""
    DROP TABLE IF EXISTS {schema}.{to};
    CREATE TABLE {schema}.{to} AS
    SELECT * FROM {schema}.{copy_from}
    """

    print("Executing...")
    print(sql)

    redshift_str = redshift_str if redshift_str else 'mssql+pyodbc://Redshift'

    engine = create_engine(redshift_str)
    engine.execute(sql)

    return "Success"


def set_cwd(*args):
    try:
        cwd = os.environ['USERPROFILE']
    except KeyError:
        cwd = "Error with UserProfile"
    cwd = os.path.join(cwd, *args)
    return cwd


def get_path(*args, from_where='python'):
    """Quick utility function to get the full path from either
    the python execution root folder or from your python
    notebook or python module folder

    Parameters
    ----------
    from_where : {'python', 'here'}, optional

        * with the python option the path starts from the
        python execution environment
        * with the here option the path starts from the
        folder in which your module or notebook is

    Returns
    -------
    str
        path in string format
    """
    if from_where == 'python':
        try:
            cwd = os.environ['USERPROFILE']
        except KeyError:
            cwd = "Error with UserProfile"
        cwd = os.path.join(cwd, *args)
        return cwd
    elif from_where == 'here':
        cwd = os.path.abspath('')
        cwd = os.path.join(cwd, *args)
        return cwd


def file_extension(file_path:str):
    """Gets extension of file.

    Parameters
    ----------
    file_path : str
        Path to the file

    Returns
    -------
    str
        File extension, eg '.csv'
    """
    return os.path.splitext(file_path)[1]
