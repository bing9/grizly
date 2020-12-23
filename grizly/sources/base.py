from abc import ABC, abstractmethod
from ..config import config as default_config, Config
import logging
from logging import Logger


class BaseObject(ABC):
    pass


class BaseReadSource(ABC):
    def __init__(self, config: Config = None, logger: Logger = None, *args, **kwargs):
        self.config = config or default_config
        self.logger = logger or logging.getLogger(__name__)

    @property
    @abstractmethod
    def con(self):
        pass

    @property
    @abstractmethod
    def objects(self):
        """List of names of objects present in the Source."""
        pass

    @abstractmethod
    def object(self, name):
        pass


class BaseWriteSource(BaseReadSource):
    @abstractmethod
    def copy_object(self):
        """*[Not implemented yet]*"""
        pass

    @abstractmethod
    def delete_object(self):
        """*[Not implemented yet]*"""
        pass

    @abstractmethod
    def create_object(self):
        """*[Not implemented yet]*"""
        pass
