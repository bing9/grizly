from abc import ABC, abstractmethod
from ..config import config as default_config, Config
import logging
from logging import Logger


class BaseObject(ABC):
    pass


class BaseSource(ABC):
    def __init__(
        self, config: Config = None, logger: Logger = None,
    ):
        self.config = config or default_config
        self.logger = logger or logging.getLogger(__name__)

    @property
    @abstractmethod
    def con(self):
        pass

    @abstractmethod
    def copy_object(self):
        pass

    @abstractmethod
    def delete_object(self):
        pass

    @abstractmethod
    def create_object(self):
        pass

    @property
    @abstractmethod
    def objects(self):
        pass

    @abstractmethod
    def object(self, name):
        pass
