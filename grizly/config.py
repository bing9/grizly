import json
from .utils import get_path
import logging


class Config:
    """Class which stores grizly configuration"""

    data = {}

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        config_path = os.environ.get("GRIZLY_CONFIG_FILE", get_path(".grizly", "config.json"))
        if os.path.exists(config_path):
            self.from_json(config_path)
        else:
            raise FileNotFoundError(f"Config file not found: {config_path}.")

    def from_json(self, json_path: str):
        """Overwrites Config.data using json file data

        Parameters
        ----------
        json_path : str
            Path to json file

        Examples
        --------
        >>> json_path = get_path('grizly_dev', 'notebooks', 'config.json')
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
                _validate_config(data["config"][key], services=None)

            Config.data = data["config"]
            self.logger.debug(f"Config data loaded from {json_path}.")
        else:
            raise KeyError(f"'config' key not found in config file {json_path}")

    def get_service(
        self, service: str, config_key: str = None, env: str = None,
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
            * 'schedule'
            * 's3'
        config_key : str, optional
            Config key, by default 'standard'
        env : str, optional
            ONLY FOR service='sfdc', options:

            * 'prod'
            * 'stage'
            * None: then 'prod' key is taken

        Examples
        --------
        >>> json_path = get_path('grizly_dev', 'notebooks', 'config.json')
        >>> conf = Config().from_json(json_path)
        >>> conf.get_service(service='email')
        {'email_address': 'my_email@example.com', 'email_password': 'my_password', 'send_as': 'Team'}
        >>> conf.get_service(service='sfdc', env='stage')
        {'username': 'my_login', 'instance_url': 'https://na1.salesforce.com', 'password': 'my_password', 'organizationId': 'OrgId'}

        Returns
        -------
        dict
            Dictionary with keys which correspond to the service.
        """
        config_key = config_key or "standard"
        env = env or "prod"

        if Config.data == {}:
            config_path = os.environ.get("GRIZLY_CONFIG_FILE")
            if config_path is not None:
                self.from_json(config_path)
            elif os.path.exists(get_path(".grizly", "config.json")):
                self.from_json(get_path(".grizly", "config.json"))
            else:
                raise FileNotFoundError(
                    "Config file not found. Please specify env variable GRIZLY_CONFIG_FILE with path to file"
                    " or load config file using Config.from_json() method."
                )

        if config_key not in Config.data.keys():
            print(Config.data)
            raise KeyError(f"Key {config_key} not found in config. Please check Config class documentation.")

        # _validate_config(self.data[config_key], services=service, env=env)
        if service == "sfdc":
            return Config.data[config_key][service][env]
        else:
            if service in Config.data[config_key]:
                return Config.data[config_key][service]
            else:
                return dict()


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
    return config
    # if not isinstance(config, dict):
    #     raise TypeError("config must be a dictionary")
    # if config == {}:
    #     raise ValueError("config is empty")

    # valid_services = {"email", "github", "sfdc", "proxies", "sqldb", "schedule"}
    # # invalid_keys = set(config.keys()) - valid_services
    # # if invalid_keys != set():
    # #     raise KeyError(f"Root invalid keys {invalid_keys} in config. Valid keys: {valid_services}")

    # if services is None:
    #     services = list(set(config.keys()).intersection(valid_services))
    # if isinstance(services, str):
    #     services = [services]
    # if not isinstance(services, list):
    #     raise TypeError("services must be a list or string")

    # invalid_services = set(services) - valid_services
    # if invalid_services != set():
    #     raise ValueError(f"Invalid values in services {invalid_services}. Valid values: {valid_services}")

    # env = env if env else "prod"
    # if env not in ("prod", "stage"):
    #     raise ValueError(f"Invalid value '{env}' in env. Valid values: 'prod', 'stage', None")

    # for service in services:
    #     if service not in config.keys():
    #         raise KeyError(f"'{service}' not found in config")
    #     if not isinstance(config[service], dict):
    #         raise TypeError(f"config['{service}'] must be a dictionary")
    #     if config[service] == {}:
    #         raise ValueError(f"config['{service}'] is empty")

    #     if service == "email":
    #         valid_keys = {"email_address", "email_password", "send_as"}
    #     elif service == "github":
    #         valid_keys = {"pages", "username", "username_password"}
    #     elif service == "sfdc":
    #         valid_keys = {"stage", "prod"}
    #     elif service == "proxies":
    #         valid_keys = {"http", "https"}
    #     elif service == "sqldb":
    #         valid_keys = {"db", "dialect"}
    #         for key in config[service]:
    #             if not isinstance(config[service][key], dict):
    #                 raise TypeError(f"config['{service}']['{key}'] must be a dictionary")
    #             if config[service][key] == {}:
    #                 raise ValueError(f"config['{service}']['{key}'] is empty")

    #             invalid_keys = set(config[service][key].keys()) - valid_keys
    #             if invalid_keys != set():
    #                 raise KeyError(
    #                     f"Invalid keys {invalid_keys} in config['{service}']['{key}']. Valid keys: {valid_keys}"
    #                 )
    #             not_found_keys = valid_keys - set(config[service][key].keys())
    #             if not_found_keys != set():
    #                 raise KeyError(f"Keys {not_found_keys} not found in config['{service}']['{key}']")
    #         return config

    #     elif service == "schedule":
    #         valid_keys = {
    #             "dsn",
    #             "schema",
    #             "job_registry_table",
    #             "job_status_table",
    #             "job_triggers_table",
    #             "job_n_triggers_table",
    #         }

    #     invalid_keys = set(config[service].keys()) - valid_keys
    #     if invalid_keys != set():
    #         raise KeyError(f"Invalid keys {invalid_keys} in config['{service}']. Valid keys: {valid_keys}")

    #     not_found_keys = valid_keys - set(config[service].keys())
    #     if not_found_keys != set() and service != "sfdc" or service == "sfdc" and env in not_found_keys:
    #         raise KeyError(f"Keys {not_found_keys} not found in config['{service}']")

    #     if service == "sfdc":
    #         if env == "stage":
    #             valid_keys = {"username", "password", "instance_url", "organizationId"}
    #         else:
    #             valid_keys = {"username", "password", "organizationId"}

    #         if env in config["sfdc"].keys():
    #             if not isinstance(config["sfdc"][env], dict):
    #                 raise TypeError(f"config['sfdc']['{env}'] must be a dictionary")
    #             if config["sfdc"][env] == {}:
    #                 raise ValueError(f"config['sfdc']['{env}'] is empty")

    #             invalid_keys = set(config["sfdc"][env].keys()) - valid_keys
    #             if invalid_keys != set():
    #                 raise KeyError(f"Invalid keys {invalid_keys} in config['sfdc']['{env}']. Valid keys: {valid_keys}")

    #             not_found_keys = valid_keys - set(config["sfdc"][env].keys())
    #             if not_found_keys != set():
    #                 raise KeyError(f"Keys {not_found_keys} not found in config['sfdc']['{env}']")
    #         else:
    #             raise KeyError(f"Key '{env}' not found in config['sfdc']")

    # return config


from sys import platform
import os

if platform.startswith("linux"):
    home_env = "HOME"
else:
    home_env = "USERPROFILE"
home_dir = os.getenv(home_env) or "/root"
default_config_dir = os.path.join(home_dir, ".grizly")


config = Config()
