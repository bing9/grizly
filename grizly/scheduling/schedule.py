from distributed import Client, Future, progress
import dask
from typing import Any, Dict, List


class JobRegistryTable:
    def register(self, job: Job):
        pass


class JobStatusTable:
    pass


class Job:
    def __init__(self, name: str, owner: str, tasks: List[dask.delayed.Delayed], env: str = None, **kwargs):
        self.name = name
        self.owner = owner
        self.tasks = tasks
        self.graph = dask.delayed()(self.tasks, name=self.name + "_graph")
        self.env = env or "local"

    def __repr__(self):
        pass

    def register(self):
        JobRegistryTable().register(job=self)

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
