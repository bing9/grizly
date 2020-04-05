import json
from .utils import get_path
import logging


class Config:
    """Class which stores grizly configuration"""

    data = {}

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def from_dict(self, data: dict):
        """Overwrites Config.data using dictionary data

        Parameters
        ----------
        data : dict
            Dictionary with config key.

        Examples
        --------
        >>> standard = {
        ...        "config": {
        ...            "standard": {
        ...            "email": {
        ...                "email_address": "my_email@example.com",
        ...                "email_password": "my_password",
        ...                "send_as": "Team"
        ...            },
        ...            "github": {
        ...                "username": "my_login",
        ...                "username_password": "my_password",
        ...                "proxies": {
        ...                     "http": "first_proxy",
        ...                     "https": "second_proxy"
        ...                },
        ...                "pages": 100
        ...            },
        ...            "sfdc": {
        ...                "stage": {
        ...                "username": "my_login",
        ...                "instance_url": "https://na1.salesforce.com",
        ...                "password": "my_password",
        ...                "organizationId": "OrgId"
        ...                },
        ...                "prod": {
        ...                "username": "my_login",
        ...                "password": "my_password",
        ...                "organizationId": "OrgId"
        ...                }
        ...            },
        ...            "proxies": {
        ...                "http": "first_proxy",
        ...                "https": "second_proxy"
        ...            },
        ...             "sqldb": {
        ...             "redshift": "mssql+pyodbc://redshift_acoe",
        ...             "denodo": "mssql+pyodbc://DenodoPROD"
        ...        }
        ...            }
        ...        }
        ...        }
        >>> conf = Config().from_dict(standard)

        Returns
        -------
        Config
        """
        if "config" in data:
            if not isinstance(data["config"], dict):
                raise TypeError("config must be a dictionary")
            if data["config"] == {}:
                raise ValueError("config is empty")

            for key in data["config"].keys():
                _validate_config(
                    data["config"][key], services=list(data["config"][key])
                )

            Config.data = data["config"]
            self.logger.debug("Config data has been saved.")
            return Config()
        else:
            raise KeyError("'config' key not found")

    def from_json(self, json_path: str):
        """Overwrites Config.data using json file data

        Parameters
        ----------
        json_path : str
            Path to json file

        Examples
        --------
        >>> json_path = get_path('dev', 'grizly', 'notebooks', 'config.json')
        >>> conf = Config().from_json(json_path)

        Returns
        -------
        Config
        """
        with open(json_path, "r") as json_file:
            data = json.load(json_file)
        if "config" in data:
            if not isinstance(data["config"], dict):
                raise TypeError("config must be a dictionary")
            if data["config"] == {}:
                raise ValueError("config is empty")

            for key in data["config"].keys():
                _validate_config(
                    data["config"][key], services=list(data["config"][key])
                )

            Config.data = data["config"]
            self.logger.debug("Config data has been saved.")
            return Config()
        else:
            raise KeyError("'config' key not found")

    def add_keys(self, data: dict, if_exists: str = "skip"):
        """Adds new keys to Config.data

        Parameters
        ----------
        data : dict
            Dictionary with keys to be added to Config.data
        if_exists : {'skip', 'replace'}, default 'skip'
            How to behave if the key already exists.

            * skip: Skips existing key
            * replace: Replaces existing key

        Examples
        --------
        >>> personal_john = {
        ...        "personal_john": {
        ...        "email": {
        ...            "email_address": "john_snow@example.com",
        ...            "email_password": "wolf123",
        ...            "send_as": "John Snow"
        ...        }
        ...        }
        ...    }
        >>> conf = Config().add_keys(personal_john)
        >>> Config.data["personal_john"]
        {'email': {'email_address': 'john_snow@example.com', 'email_password': 'wolf123', 'send_as': 'John Snow'}}

        Returns
        -------
        Config
        """
        if if_exists not in ("skip", "replace"):
            raise ValueError(
                "'{}' is not valid for if_exists. Valid values: 'skip', 'replace'".format(
                    if_exists
                )
            )
        for key in data.keys():
            if key in list(Config.data.keys()):
                if if_exists == "skip":
                    print(
                        f"Key '{key}' already exists and has been skipped. If you want to overwrite it please use if_exists='replace'"
                    )
                elif if_exists == "replace":
                    _validate_config(data[key], services=list(data[key]))
                    Config.data.update({key: data[key]})
                    print(f"Key '{key}' has been overwritten.")
            else:
                _validate_config(data[key], services=list(data[key]))
                Config.data[key] = data[key]
                self.logger.debug(f"Key '{key}' has been added.")
        return Config()

    def get_service(
        self,
        service: {"email", "github", "sfdc", "proxies", "sqldb"},
        config_key: str = None,
        env: str = None,
    ):
        """Returns dictionary data for given service and config key.

        Parameters
        ----------
        service : str
            Services, options:

            * 'email'
            * 'github'
            * 'sfdc'
            * 'proxies'
            * 'sqldb'
        config_key : str, optional
            Config key, by default 'standard'
        env : str, optional
            ONLY FOR service='sfdc', options:

            * 'prod'
            * 'stage'
            * None: then 'prod' key is taken

        Examples
        --------
        >>> json_path = get_path('dev', 'grizly', 'notebooks', 'config.json')
        >>> conf = Config().from_json(json_path)
        >>> conf.get_service(service='email')
        {'email_address': 'my_email@example.com',
        'email_password': 'my_password',
        'send_as': 'Team'}
        >>> conf.get_service(service='sfdc', env='stage')
        {'username': 'my_login',
        'instance_url': 'https://na1.salesforce.com',
        'password': 'my_password',
        'organizationId': 'OrgId'}

        Returns
        -------
        dict
            Dictionary with keys which correspond to the service.
        """
        config_key = config_key or "standard"
        env = env or "prod"

        if config_key not in Config.data.keys():
            raise KeyError(
                f"Key {config_key} not found in config. Please check Config class documentation."
            )

        _validate_config(self.data[config_key], services=service, env=env)
        if service == "sfdc":
            return Config.data[config_key][service][env]
        else:
            return Config.data[config_key][service]


def _validate_config(config: dict, services: list = None, env: str = None):
    """Validates config dictionary.

    Parameters
    ----------
    config : dict
        Config to validate
    service : str or list
        Services to validate, options:

        * 'email'
        * 'github'
        * 'sfdc'
        * 'proxies'
        * 'sqldb'
        * None: then ['email', 'github', 'sfdc', 'proxies', 'sqldb']
    env : str
        ONLY FOR service='sfdc', options:

        * 'prod'
        * 'stage'
        * None: then 'prod' key is validated

    Returns
    -------
    dict
        Validated config
    """
    if not isinstance(config, dict):
        raise TypeError("config must be a dictionary")
    if config == {}:
        raise ValueError("config is empty")

    valid_services = {"email", "github", "sfdc", "proxies", "sqldb"}
    invalid_keys = set(config.keys()) - valid_services
    if invalid_keys != set():
        raise KeyError(
            f"Root invalid keys {invalid_keys} in config. Valid keys: {valid_services}"
        )

    if services == None:
        services = list(valid_services)
    if isinstance(services, str):
        services = [services]
    if not isinstance(services, list):
        raise TypeError("services must be a list or string")

    invalid_services = set(services) - valid_services
    if invalid_services != set():
        raise ValueError(
            f"Invalid values in services {invalid_services}. Valid values: {valid_services}"
        )

    env = env if env else "prod"
    if env not in ("prod", "stage"):
        raise ValueError(
            f"Invalid value '{env}' in env. Valid values: 'prod', 'stage', None"
        )

    for service in services:
        if service not in config.keys():
            raise KeyError(f"'{service}' not found in config")
        if not isinstance(config[service], dict):
            raise TypeError(f"config['{service}'] must be a dictionary")
        if config[service] == {}:
            raise ValueError(f"config['{service}'] is empty")

        if service == "email":
            valid_keys = {"email_address", "email_password", "send_as"}
        elif service == "github":
            valid_keys = {"pages", "proxies", "username", "username_password"}
        elif service == "sfdc":
            valid_keys = {"stage", "prod"}
        elif service == "proxies":
            valid_keys = {"http", "https"}
        elif service == "sqldb":
            valid_keys = {"redshift", "denodo"}

        invalid_keys = set(config[service].keys()) - valid_keys
        if invalid_keys != set():
            raise KeyError(
                f"Invalid keys {invalid_keys} in config['{service}']. Valid keys: {valid_keys}"
            )

        not_found_keys = valid_keys - set(config[service].keys())
        if (
            not_found_keys != set()
            and service != "sfdc"
            or service == "sfdc"
            and env in not_found_keys
        ):
            raise KeyError(f"Keys {not_found_keys} not found in config['{service}']")

        if service == "sfdc":
            if env == "stage":
                valid_keys = {"username", "password", "instance_url", "organizationId"}
            else:
                valid_keys = {"username", "password", "organizationId"}

            if env in config["sfdc"].keys():
                if not isinstance(config["sfdc"][env], dict):
                    raise TypeError(f"config['sfdc']['{env}'] must be a dictionary")
                if config["sfdc"][env] == {}:
                    raise ValueError(f"config['sfdc']['{env}'] is empty")

                invalid_keys = set(config["sfdc"][env].keys()) - valid_keys
                if invalid_keys != set():
                    raise KeyError(
                        f"Invalid keys {invalid_keys} in config['sfdc']['{env}']. Valid keys: {valid_keys}"
                    )

                not_found_keys = valid_keys - set(config["sfdc"][env].keys())
                if not_found_keys != set():
                    raise KeyError(
                        f"Keys {not_found_keys} not found in config['sfdc']['{env}']"
                    )
            else:
                raise KeyError(f"Key '{env}' not found in config['sfdc']")

    return config


from sys import platform
import os

if platform.startswith("linux"):
    home_env = "HOME"
else:
    home_env = "USERPROFILE"
home_dir = os.getenv(home_env) or "/root"
default_config_dir = os.path.join(home_dir, ".grizly")
