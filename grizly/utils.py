import os
import pandas as pd
from simple_salesforce import Salesforce
from simple_salesforce.login import SalesforceAuthenticationFailed
from sys import platform
import json

from functools import partial
import deprecation
import logging

deprecation.deprecated = partial(
    deprecation.deprecated, deprecated_in="0.3", removed_in="0.4"
)

logger = logging.getLogger(__name__)


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

    config = read_config()

    sfdc_username = config["sfdc_username"]
    sfdc_pw = config["sfdc_password"]

    try:
        sf = Salesforce(
            password=sfdc_pw, username=sfdc_username, organizationId="00DE0000000Hkve"
        )
    except SalesforceAuthenticationFailed:
        logger.info(
            "Could not log in to SFDC. Are you sure your password hasn't expired and your proxy is set up correctly?"
        )
        raise SalesforceAuthenticationFailed
    field_descriptions = eval(
        f'sf.{table}.describe()["fields"]'
    )  # change to variable table
    types = {
        field["name"]: (field["type"], field["length"]) for field in field_descriptions
    }

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


def set_cwd(*args):
    try:
        cwd = os.environ["USERPROFILE"]
    except KeyError:
        cwd = "Error with UserProfile"
    cwd = os.path.join(cwd, *args)
    return cwd


def get_path(*args, from_where="python"):
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
    if from_where == "python":
        if platform.startswith("linux"):
            home_env = "HOME"
        elif platform.startswith("win"):
            home_env = "USERPROFILE"
        else:
            raise NotImplementedError(
                f"Unable to retrieve home env variable for {platform}"
            )

        home_path = os.getenv(home_env) or "/root"
        cwd = os.path.join(home_path, *args)
        return cwd

    elif from_where == "here":
        cwd = os.path.abspath("")
        cwd = os.path.join(cwd, *args)
        return cwd


def file_extension(file_path: str):
    """Gets extension of file.

    Parameters
    ----------
    file_path : str
        Path to the file

    Returns
    -------
    str
        File extension, eg 'csv'
    """
    return os.path.splitext(file_path)[1][1:]


def clean_colnames(df):

    reserved_words = ["user"]

    df.columns = df.columns.str.strip().str.replace(
        " ", "_"
    )  # Redshift won't accept column names with spaces
    df.columns = [
        f'"{col}"' if col.lower() in reserved_words else col for col in df.columns
    ]

    return df


def clean(df):
    def remove_inside_quotes(string):
        """ removes double single quotes ('') inside a string,
        e.g. Sam 'Sammy' Johnson -> Sam Sammy Johnson """

        # pandas often parses timestamp values obtained from SQL as objects
        if type(string) == pd.Timestamp:
            return string

        if pd.notna(string):
            if isinstance(string, str):
                if string.find("'") != -1:
                    first_quote_loc = string.find("'")
                    if string.find("'", first_quote_loc + 1) != -1:
                        second_quote_loc = string.find("'", first_quote_loc + 1)
                        string_cleaned = (
                            string[:first_quote_loc]
                            + string[first_quote_loc + 1 : second_quote_loc]
                            + string[second_quote_loc + 1 :]
                        )
                        return string_cleaned
        return string

    def remove_inside_single_quote(string):
        """ removes a single single quote ('') from the beginning of a string,
        e.g. Sam 'Sammy' Johnson -> Sam Sammy Johnson """
        if type(string) == pd.Timestamp:
            return string

        if pd.notna(string):
            if isinstance(string, str):
                if string.startswith("'"):
                    return string[1:]
        return string

    df_string_cols = df.select_dtypes(object)
    df_string_cols = (
        df_string_cols.applymap(remove_inside_quotes)
        .applymap(remove_inside_single_quote)
        .replace(to_replace="\\", value="")
        .replace(
            to_replace="\n", value="", regex=True
        )  # regex=True means "find anywhere within the string"
    )
    df.loc[:, df.columns.isin(df_string_cols.columns)] = df_string_cols

    bool_cols = df.select_dtypes(bool).columns
    df[bool_cols] = df[bool_cols].astype(int)

    return df


@deprecation.deprecated(
    details="Use Config().from_json function or in case of AWS credentials - start using S3 class !!!",
)
def read_config():
    if platform.startswith("linux"):
        home_env = "HOME"
    else:
        home_env = "USERPROFILE"
    default_config_dir = os.path.join(os.environ[home_env], ".grizly")

    try:
        json_path = os.path.join(default_config_dir, "etl_config.json")
        with open(json_path, "r") as f:
            config = json.load(f)
    except KeyError:
        config = "Error with UserProfile"
    return config
