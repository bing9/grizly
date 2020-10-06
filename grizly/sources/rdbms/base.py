from ..base import BaseSource
from abc import ABC, ABCMeta, abstractmethod


class RDBMS(BaseSource, metaclass=ABCMeta):
    @abstractmethod
    def table(self):
        pass

class BaseTable(ABC):
    def __init__(self, name, source, schema=None):
        self.name = name
        self.source = source
        self.schema = schema
        self.fully_qualified_name = name if not schema else f"{schema}.{name}"

    def __repr__(self):
        return f"{self.__class__.__name__}(\"{self.name}\")"

    def info(self):
        print(f"""
        Table: {self.fully_qualified_name}
        Fields: {self.ncols}
        Rows: {self.nrows}
        """)

    @property
    @abstractmethod
    def fields(self):
        pass

    @property
    def columns(self):
        """Alias for fields"""
        return self.fields

    @property
    @abstractmethod
    def types(self):
        pass

    @property
    def dtypes(self):
        """Alias for types"""
        return self.types

    @property
    @abstractmethod
    def nrows(self):
        pass

    @property
    @abstractmethod
    def ncols(self):
        pass

    def __len__(self):
        return self.nrows
