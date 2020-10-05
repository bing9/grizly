from abc import ABC
from ..config import config as default_config, Config
import logging
from logging import Logger


class BaseSource(ABC):
    def __init__(
        self, config: Config = None, logger: Logger = None,
    ):
        self.config = config or default_config
        self.logger = logger or logging.getLogger(__name__)

    @property
    def con(self):
        pass

    def copy_object(self):
        pass

    def delete_object(self):
        pass

    def create_object(self):
        pass

    def get_objects(self):
        pass
