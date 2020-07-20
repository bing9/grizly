from distributed import Client, Future, progress
import dask
import logging
from typing import Any, Dict, List

from ..tools.sqldb import SQLDB
from .tables import JobRegistryTable


class Job:
    def __init__(
        self,
        name: str,
        owner: str,
        tasks: List[dask.delayed],
        source: Dict[str, Any] = None,
        source_type: str = None,
        type: str = None,
        trigger: Dict[str, Any] = None,
        notification: Dict[str, Any] = None,
        env: str = None,
        logger: logging.Logger = None,
        **kwargs,
    ):
        self.name = name
        self.owner = owner
        self.source = source
        self.source_type = source_type
        self.type = type
        self.tasks = tasks
        self.graph = dask.delayed()(self.tasks, name=self.name + "_graph")
        self.trigger = trigger
        self.notification = notification
        self.env = env or "local"
        self.logger = logger or logging.getLogger(__name__)

    # @property
    # def source_type(self):
    #     if self.source.lower().startswith("https://github.com"):
    #         return "github"
    #     elif os.path.exists(self.source):
    #         return "local"
    #     else:
    #         raise NotImplementedError(f"Source {self.source} not supported")

    def __repr__(self):
        pass

    def register(self, **kwargs):
        JobRegistryTable(logger=self.logger, **kwargs).register(job=self)

    def visualize(self, **kwargs):
        return self.graph.visualize(**kwargs)

    def submit(
        self,
        client: Client = None,
        scheduler_address: str = None,
        priority: int = None,
        resources: Dict[str, Any] = None,
    ) -> None:

        priority = priority or 1
        if not client:
            client = Client(scheduler_address)

        self.scheduler_address = client.scheduler.address

        computation = client.compute(self.graph, retries=3, priority=priority, resources=resources)
        progress(computation)
        dask.distributed.fire_and_forget(computation)
        if not client:  # if cient is provided, we assume the user will close it
            client.close()
        return None

    def cancel(self, scheduler_address=None):
        if not scheduler_address:
            scheduler_address = self.scheduler_address
        client = Client(scheduler_address)
        f = Future(self.name + "_graph", client=client)
        f.cancel(force=True)
        client.close()

