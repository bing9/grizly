import json
import logging
import os
from functools import partial, wraps
from sys import platform
from time import sleep
from typing import TypeVar

import deprecation
import pandas as pd
from simple_salesforce import Salesforce
from simple_salesforce.login import SalesforceAuthenticationFailed

from .type_mappers import sfdc_to_sqlalchemy

deprecation.deprecated = partial(deprecation.deprecated, deprecated_in="0.3", removed_in="0.4")

logger = logging.getLogger(__name__)


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
        sf = Salesforce(password=sfdc_pw, username=sfdc_username, organizationId="00DE0000000Hkve")
    except SalesforceAuthenticationFailed:
        logger.info(
            "Could not log in to SFDC."
            "Are you sure your password hasn't expired and your proxy is set up correctly?"
        )
        raise SalesforceAuthenticationFailed
    table = getattr(sf, table)
    field_descriptions = table.describe()["fields"]
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
            field_sqlalchemy_type = sfdc_to_sqlalchemy(field_sfdc_type)
            if field_sqlalchemy_type == "NVARCHAR":
                field_sqlalchemy_type = f"{field_sqlalchemy_type}({field_len})"
            dtypes[field] = field_sqlalchemy_type
        return dtypes
    else:
        raise NotImplementedError("Retrieving columns only is currently not supported")


def get_path(*args, from_where="python") -> str:
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
            raise NotImplementedError(f"Unable to retrieve home env variable for {platform}")

        home_path = os.getenv(home_env) or "/root"
        cwd = os.path.join(home_path, *args)
        return cwd

    else:
        cwd = os.path.abspath("")
        cwd = os.path.join(cwd, *args)
        return cwd


def set_cwd(*args):
    return get_path(*args)


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
    df.columns = [f'"{col}"' if col.lower() in reserved_words else col for col in df.columns]

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


def retry(exceptions, tries=4, delay=3, backoff=2, logger=None):
    """
    Retry calling the decorated function using an exponential backoff.

    Args:
        exceptions: The exception to check. may be a tuple of
            exceptions to check.
        tries: Number of times to try (not retry) before giving up.
        delay: Initial delay between retries in seconds.
        backoff: Backoff multiplier (e.g. value of 2 will double the delay
            each retry).
        logger: Logger to use. If None, print.


    This is almost a copy of Workflow.retry, but it's using its own logger.
    """
    if not logger:
        logger = logging.getLogger(__name__)

    def deco_retry(f):
        @wraps(f)
        def f_retry(*args, **kwargs):

            mtries, mdelay = tries, delay

            while mtries > 1:

                try:
                    return f(*args, **kwargs)

                except exceptions as e:
                    msg = f"{e}, \nRetrying in {mdelay} seconds..."
                    logger.warning(msg)
                    sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff

            return f(*args, **kwargs)

        return f_retry  # true decorator

    return deco_retry


def none_safe_loads(value):
    if value is not None:
        return json.loads(value)


def dict_diff(first: dict, second: dict, by: str = "keys") -> dict:
    """Inner join of two dictionaries"""
    if by == "keys":
        diff = {k: second[k] for k in set(second) - set(first)}
    elif by == "values":
        diff = {k: second[k] for k, _ in set(second.items()) - set(first.items())}
    else:
        raise NotImplementedError("Can only compare by keys or values.")
    return diff


def isinstance2(obj, _cls):
    """Work around isinstance() not working with python's standard typing module"""
    if isinstance(_cls, TypeVar):
        obj_class = obj.__class__.__name__
        cls_class = _cls.__name__
        parents = [parent_cls.__name__ for parent_cls in type(obj).__bases__]
        for parent_class in parents:
            if parent_class == cls_class:
                return True
        return obj_class == cls_class
    else:
        return isinstance(obj, _cls)
