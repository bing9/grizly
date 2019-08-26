import os
import json
from sqlalchemy import create_engine
import pandas as pd
from sqlalchemy.pool import NullPool


def read_config():
    json_path = os.path.join(os.environ['USERPROFILE'], '.grizly', 'etl_config.json')
    with open(json_path, 'r') as f:
                config = json.load(f)
    return config


config = read_config()
os.environ["HTTPS_PROXY"] = config["https"]

def columns_to_excel(table, excel_path, schema):
    """
    Get columns from Denodo table
    """
    query = f"""
        SELECT column_name
        FROM get_view_columns()
        WHERE view_name = '{table}'
            AND database_name = '{schema}'
        """

    engine = create_engine("mssql+pyodbc://DenodoODBC")
    col_names = pd.read_sql(query, engine)
    col_names.to_excel(excel_path, index=False)
    return "Columns saved in excel."


def check_if_exists(table, schema=''):
    """
    Checks if a table exists in Redshift.
    """
    engine = create_engine("mssql+pyodbc://Redshift", encoding='utf8', poolclass=NullPool)
    if schema == '':
        table_name = table
        sql_exists = "select * from information_schema.tables where table_name = '{}' ". format(table)
    else:
        table_name = schema + '.' + table
        sql_exists = "select * from information_schema.tables where table_schema = '{}' and table_name = '{}' ". format(schema, table)

    return not pd.read_sql_query(sql = sql_exists, con=engine).empty


def delete_where(table, schema='', *argv):
    """
    Removes records from Redshift table which satisfy *argv.

    Parameters:
    ----------
    table : string
        Name of SQL table.
    schema : string, optional
        Specify the schema.

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

    if check_if_exists(table, schema):
        engine = create_engine("mssql+pyodbc://Redshift", encoding='utf8', poolclass=NullPool)

        if argv is not None:
            for arg in argv:
                sql = f"DELETE FROM {table_name} WHERE {arg} "
                engine.execute(sql)
                print(f'Records from table {table_name} where {arg} has been removed successfully.')
    else:
        print(f"Table {table_name} doesn't exist.")


def copy_table(schema, copy_from, to, engine=None):

    sql = f"""
    DROP TABLE IF EXISTS {schema}.{to};
    CREATE TABLE {schema}.{to} AS
    SELECT * FROM {schema}.{copy_from}
    """

    print("Executing...")
    print(sql)

    if engine is None:
        engine = create_engine("mssql+pyodbc://Redshift")

    engine.execute(sql)

    return "Success"

