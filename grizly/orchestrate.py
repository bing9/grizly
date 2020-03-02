import datetime as dt
import json
import logging
import os
import sys
import traceback
from datetime import date, datetime, timedelta, timezone
from functools import wraps
from json.decoder import JSONDecodeError
from logging import Logger
from time import sleep, time
from typing import Any, Dict, Iterable, List
from distributed import Client
from grizly.tools.s3 import S3

import dask
import graphviz
import pandas as pd
from croniter import croniter
from dask.core import get_dependencies
from dask.distributed import fire_and_forget
from dask.dot import _get_display_cls
from dask.optimization import key_split
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool

from grizly.config import Config

from .email import Email, EmailAccount
from .etl import df_to_s3, s3_to_rds
from .utils import get_last_working_day, read_config

workflows_dir = "/home/acoe_workflows"
LISTENER_STORE = os.path.join(workflows_dir, "workflows", "etc", "listener_store.json")

# logger = logging.getLogger(__name__)

def cast_to_date(maybe_date: Any) -> dt.date:
    """
    Casts a date/datetime-like value to a Date object.

    Parameters
    ----------
    maybe_date : Any
        The date/datetime-like value to be conveted to a Date object.

    Returns
    -------
    _date
        The date-like object converted to an actual Date object.

    Raises
    ------
    TypeError
        When maybe_date is not of one of the supported types.
    """

    def _validate_date(maybe_date: Any) -> bool:
        if not isinstance(maybe_date, (dt.date, int)):
            return False
        return True

    if isinstance(maybe_date, str):
        _date = datetime.strptime(maybe_date, "%Y-%m-%d").date()
    elif isinstance(maybe_date, datetime):
        _date = datetime.date(maybe_date)
    else:
        _date = maybe_date

    if not _validate_date(_date):
        raise TypeError(f"The specified trigger field is not of type (int, date)")

    return _date


class Schedule:
    """
    Cron schedule.
    Args:
        - cron (str): cron string
        - start_date (datetime, optional): an optional schedule start datetime (assumed to be UTC)
        - end_date (datetime, optional): an optional schedule end date
    Raises:
        - ValueError: if the cron string is invalid
    """

    def __init__(self, cron, start_date=None, end_date=None):
        if not croniter.is_valid(cron):
            raise ValueError("Invalid cron string: {}".format(cron))
        self.cron = cron
        self.start_date = start_date
        self.end_date = end_date

    def __repr__(self):
        return f"{type(self).__name__}({self.cron})"

    def emit_dates(self, start_date: datetime = None) -> Iterable[datetime]:
        """
        Generator that emits workflow run dates
        Args:
            - start_date (datetime, optional): an optional schedule start date
        Returns:
            - Iterable[datetime]: the next scheduled dates
        """
        if not self.start_date:
            start_date = datetime.now(timezone.utc)

        cron = croniter(self.cron, start_date)

        while True:
            next_date = cron.get_next(datetime).replace(tzinfo=timezone.utc)
            if self.end_date and next_date > self.end_date:
                break
            yield next_date

    def next(self, n):

        dates = []
        for date in self.emit_dates(start_date=self.start_date):
            dates.append(date)
            if len(dates) == n:
                break

        return dates


class Trigger:
    def __init__(self, func, params=None):
        self.check = func
        self.kwargs = params or {}
        self.table = params.get("table")
        self.schema = params.get("schema")

    @property
    def last_table_refresh(self):
        return self.check(**self.kwargs)

    @property
    def should_run(self):
        return bool(self.last_table_refresh)


class Listener:
    """
    A class that listens for changes in a table and server as a trigger for upstream-dependent workflows.

    Checks and stores table's last refresh/trigger date.
    """

    def __init__(
        self,
        workflow,
        schema=None,
        table=None,
        field=None,
        query=None,
        db="denodo",
        trigger=None,
        delay=0,
    ):

        self.workflow = workflow
        self.name = workflow.name
        self.db = db
        self.schema = trigger.schema if trigger else schema
        self.table = table or trigger.table
        self.field = field
        self.query = query
        self.logger = logging.getLogger(__name__)
        self.trigger = trigger
        self.last_data_refresh = self.get_last_json_refresh(key="last_data_refresh")
        self.engine = self.get_engine()
        self.last_trigger_run = self.get_last_json_refresh(key="last_trigger_run")
        self.delay = delay
        self.config_key = "standard"

    def __repr__(self):
        if self.query:
            return f"{type(self).__name__}(query=\"\"\"{self.query}\"\"\")"
        return f"{type(self).__name__}(db={self.db}, schema={self.schema}, table={self.table}, field={self.field})"

    def retry_task(exceptions, tries=4, delay=3, backoff=2, logger=None):
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

    def get_engine(self):

        config = read_config()
        engine_str = config[self.db]
        engine = create_engine(engine_str, encoding="utf8", poolclass=NullPool)

        return engine

    def get_last_json_refresh(self, key):
        with open(LISTENER_STORE) as f:
            listener_store = json.load(f)
            if not listener_store.get(self.name):
                return None
            last_json_refresh = listener_store[self.name].get(key)  # int or serialized date
            try:
                # attempt to convert the serialized datetime to a date object
                last_json_refresh = datetime.date(datetime.strptime(last_json_refresh, r"%Y-%m-%d"))
            except:
                pass
            return last_json_refresh

    def update_json(self):
        with open(LISTENER_STORE) as json_file:
            try:
                listener_store = json.load(json_file)
            except JSONDecodeError:
                listener_store = {}
        if not isinstance(self, EmailListener):  # to be done properly with TriggerListener subclass
            if self.trigger:
                listener_store[self.name] = {"last_trigger_run": str(self.last_trigger_run)}
            else:
                if isinstance(self.last_data_refresh, dt.date):
                    listener_store[self.name] = {"last_data_refresh": str(self.last_data_refresh)}
                else:
                    listener_store[self.name] = {"last_data_refresh": self.last_data_refresh}
        else:
            if isinstance(self.last_data_refresh, dt.date):
                listener_store[self.name] = {"last_data_refresh": str(self.last_data_refresh)}
            else:
                listener_store[self.name] = {"last_data_refresh": self.last_data_refresh}

        with open(LISTENER_STORE, "w") as f_write:
            json.dump(listener_store, f_write, indent=4)

    @retry_task(TypeError, tries=3, delay=10)
    def get_table_refresh_date(self):
        if self.query:
            sql = self.query
        else:
            sql = f"SELECT {self.field} FROM {self.schema}.{self.table} ORDER BY {self.field} DESC LIMIT 1;"

        con = get_con(engine=self.engine)
        cursor = con.cursor()
        cursor.execute(sql)

        try:
            table_refresh_date = cursor.fetchone()[0]
        except TypeError:
            self.logger.exception(f"{self.name}'s trigger field is empty")
            raise

        cursor.close()
        con.close()

        # try casting to date
        table_refresh_date = cast_to_date(table_refresh_date)

        return table_refresh_date

    def should_trigger(self, table_refresh_date=None):

        if not isinstance(self, EmailListener):  # to be done properly with TriggerListener subclass
            if self.trigger:
                today = datetime.today().date()
                if today == self.last_trigger_run:
                    return False  # workflow was already ran today
                return self.trigger.should_run

        self.logger.info(f"{self.name}: last data refresh: {self.last_data_refresh}, table_refresh_date: {table_refresh_date}")
        # first run
        if not self.last_data_refresh:
            return True
        # the table ran previously, but now the refresh date is None
        # this means the table is being cleared in preparation for update
        if not table_refresh_date:
            return False
        # trigger on day change
        if table_refresh_date != self.last_data_refresh:
            return True

    def detect_change(self) -> bool:
        """Determine whether the listener should trigger a worfklow run.

        Returns
        -------
        bool
            Whether the field (specified by a Trigger function or the field parameter) has changed since it was last checked,
            as determined by Listener.should_trigger().

        Raises
        ------
        ValueError
            If neither of [field, query, trigger] is provided.
        """

        if not isinstance(self, EmailListener):  # to be done properly with adding more subclasses
            if not any([self.field, self.query, self.trigger]):
                raise ValueError("Please specify the trigger for the listener")
            if self.trigger:
                if self.should_trigger():
                    today = datetime.today().date()
                    self.last_trigger_run = today
                    self.update_json()
                    return True
        try:
            table_refresh_date = self.get_table_refresh_date()
        except:
            if isinstance(self, EmailListener):
                self.logger.exception(
                    f"Could not retrieve refresh date from {self.search_email_address}'s {self.email_folder} folder"
                )
            else:
                self.logger.exception(f"Connection or query error when connecting to {self.db}")
            table_refresh_date = None

        if self.should_trigger(table_refresh_date):
            self.last_data_refresh = table_refresh_date
            self.update_json()
            sleep(self.delay)
            return True

        return False


class EmailListener(Listener):
    def __init__(
        self,
        workflow,
        schema=None,
        table=None,
        field=None,
        query=None,
        db=None,
        trigger=None,
        delay=0,
        notification_title=None,
        email_folder=None,
        search_email_address=None,
        email_address=None,
        email_password=None,
        proxy=None,
    ):
        self.name = workflow.name
        self.notification_title = notification_title or workflow.name.lower().replace(" ", "_")
        self.db = db
        self.logger = logging.getLogger(__name__)
        self.engine = None
        self.last_trigger_run = self.get_last_json_refresh(key="last_trigger_run")
        self.delay = delay
        self.config_key = "standard"
        self.email_folder = email_folder
        self.search_email_address = search_email_address
        self.email_address = email_address
        self.email_password = email_password
        self.proxy = proxy
        self.last_data_refresh = self.get_last_json_refresh(key="last_data_refresh")
        
    def __repr__(self):
        return f"{type(self).__name__}(notification_title={self.notification_title}, search_email_address={self.search_email_address}, email_folder={self.email_folder})"

    def _validate_folder(self, account, folder_name):
        if not folder_name:
            return True
        try:
            folder = account.inbox / folder_name
        except ErrorFolderNotFound:
            raise

    def get_table_refresh_date(self):
        return self.get_last_email_date(
            self.notification_title,
            self.email_folder,
            self.search_email_address,
            self.email_address,
            self.email_password,
            self.config_key,
        )

    def get_last_email_date(
        self,
        notification_title,
        email_folder=None,
        search_email_address=None,
        email_address=None,
        email_password=None,
        config_key="standard",
    ):

        account = EmailAccount(email_address, email_password, alias=self.search_email_address, proxy=self.proxy).account
        self._validate_folder(account, email_folder)
        last_message = None

        if email_folder:
            try:
                last_message = (
                    account.inbox.glob("**/" + email_folder)
                    .filter(subject=notification_title)
                    .order_by("-datetime_received")
                    .only("datetime_received")[0]
                )
            except IndexError:
                self.logger.warning(
                    f"No notifications for {self.name} were found in {self.search_email_address}'s {email_folder} folder"
                )
        else:
            try:
                last_message = (
                    account.inbox.filter(subject=notification_title)
                    .filter(subject=notification_title)
                    .order_by("-datetime_received")
                    .only("datetime_received")[0]
                )
            except IndexError:
                self.logger.warning(f"No notifications for {self.name} were found in Inbox folder")

        if not last_message:
            return None

        d = last_message.datetime_received.date()
        last_received_date = date(d.year, d.month, d.day)

        return last_received_date


class Workflow:
    """
    A class for running Dask tasks.
    """

    def __init__(
        self,
        name,
        tasks,
        owner_email=None,
        backup_email=None,
        children=None,
        priority=0,
        trigger=None,
        trigger_type="manual",
        execution_options: dict = None
    ):
        self.name = name
        self.owner_email = owner_email
        self.backup_email = backup_email
        self.tasks = [tasks]
        self.children = children
        self.execution_options = execution_options
        self.graph = dask.delayed()(self.tasks, name=self.name+"_graph")
        self.status = "pending"
        self.is_scheduled = False
        self.is_triggered = False
        self.is_manual = False
        self.env = "prod"
        self.logger = logging.getLogger(__name__)
        self.error_value = None
        self.error_type = None
        self.priority = priority
        self.trigger = trigger
        self.trigger_type = trigger_type

        self.logger.info(f"Workflow {self.name} initiated successfully")

    def retry_task(exceptions, tries=4, delay=3, backoff=2, logger=None):
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

    def __str__(self):
        return self.tasks

    def visualize(self):
        return self.graph.visualize()

    def add_trigger(self, trigger):
        self.trigger = trigger
        self.trigger_type = type(trigger)
        if isinstance(trigger, Schedule):
            self.next_run = self.trigger.next(1)[0] # should prbably keep in Scheduele and not propagate here

    def add_schedule(self, schedule):
        self.schedule = schedule
        self.next_run = self.schedule.next(1)[0]
        self.is_scheduled = True

    def add_listener(self, listener):
        self.listener = listener
        self.is_triggered = True

    def submit(self, client, priority=None):

        if not priority:
            priority = self.priority

        computation = client.compute(self.graph, retries=3, priority=priority)
        self.status = computation.status
        fire_and_forget(computation)
        client.close()

        schema = "administration"
        table = "workflow_queue"
        engine = os.getenv("QUEUE_ENGINE") or "mssql+pyodbc://redshift_acoe"
        self.submit_to_queue(engine, schema, table, priority)
        return computation

    @retry_task(Exception, tries=3, delay=10)
    def submit_to_queue(self, engine: str, schema: str, table: str, priority: int):

        now_utc = datetime.now(timezone.utc).replace(microsecond=0)

        sql = f"""INSERT INTO {schema}.{table} (workflow_name, priority, submitted) VALUES (
        '{self.name}',
        {priority},
        '{now_utc}'
        )"""
        sqla_engine = create_engine(engine)
        execute_query(sqla_engine, sql)

        self.logger.info(f"{self.name} has been uploaded to workflow_queue")

        return None


class Runner:
    """Workflow runner"""

    def __init__(self, logger: Logger = None, env: str = "prod") -> None:
        self.env = env
        self.logger = logging.getLogger(__name__)
        self.run_params = None

    def should_run(self, workflow: Workflow) -> bool:
        """Determines whether a workflow should run based on its next scheduled run.

        Parameters
        ----------
        workflow : Workflow
            A workflow instance. This function assumes the instance is generated on scheduler tick.
            Currently, this is accomplished by calling wf.generate_workflow() on each workflow every time
            Runner.get_pending_workflows() is called,
            which means wf.next_run recalculated every time should_run() is called

        Returns
        -------
        bool
            Whether the workflow's scheduled next run coincides with current time
        """
        
        if isinstance(workflow.trigger, Schedule):
        # if workflow.is_scheduled:
            next_run_short = workflow.next_run.strftime("%Y-%m-%d %H:%M")
            now = datetime.now(timezone.utc)
            
            self.logger.info(
                f"Determining whether scheduled workflow {workflow.name} shuld run... (next scheduled run: {next_run_short})"
            )
            # self.logger.info(f"Day: {type(workflow.next_run.day)}:{workflow.next_run.day} vs now: {type(now.day)}:{now.day}")
            # self.logger.info(f"Hour: {type(workflow.next_run.hour)}:{workflow.next_run.hour} vs now: {type(now.hour)}:{now.hour}")
            # self.logger.info(f"Minute: {type(workflow.next_run.minute)}:{workflow.next_run.minute} vs now: {type(now.minute)}:{now.minute}")
            
            next_run = workflow.next_run
            if (
                    (next_run.day == now.day) and 
                    (next_run.hour == now.hour) and 
                    (next_run.minute == now.minute + 1)  # if we don't add 1 here, cron will just skip to next date once it hits now
                ): # minutes for precise scheduling - this assumes runner runs for less than 1 min
                # ideally, each listener should run in a separate thread so that this is guaranteed
                # (now, if e.g. Denodo is not responding, a scheduled workflow that comes below the triggered one will not run)
                workflow.next_run = workflow.trigger.next(1)[0]
                return True
        
        elif isinstance(workflow.trigger, Listener):
        # elif workflow.is_triggered:
            
            #listener = workflow.listener
            listener = workflow.trigger

            if isinstance(listener, EmailListener):
                folder = listener.email_folder or "inbox"
                self.logger.info(f"Listening for changes in {listener.search_email_address}'s {folder} folder")
            else:
                self.logger.info(f"Listening for changes in {listener.table}...")
            
            if listener.detect_change():
                return True

        elif workflow.is_manual:
            return True

        return False


    def overwrite_params(self, workflow: Workflow, params: Dict) -> None:
        """Overwrites specified workflow's parameters

        Parameters
        ----------
        workflow : Workflow
            The workflow to modify.
        params : Dict
            Parameters and values which will be used to modify the Workflow instance.
        """
        # params: dict fof the form {"listener": {"delay": 0}, "backup_email": "test@example.com"}
        for param in params:

            # modify parameters of classes stored in Workflow, e.g. Listener of Schedule
            if type(params[param]) == dict:
                _class = param

                # only apply listener params to triggered workflows
                if _class == "listener":
                    if not workflow.is_triggered:
                        continue

                # only apply schedule params to scheduled workflows
                elif _class == "schedule":
                    if not workflow.is_scheduled:
                        continue

                # retrieve object and names of attributes to set
                obj = eval(f"workflow.{_class}")
                obj_params = params[param]

                for obj_param in obj_params:
                    new_param_value = obj_params[obj_param]
                    setattr(obj, obj_param, new_param_value)

            # modify Workflow object's parameters
            else:
                setattr(workflow, param, params[param])


    def run(self, workflows: List[Workflow], overwrite_params: Dict = None) -> Dict:
        """[summary]

        Parameters
        ----------
        workflows : List[Workflow]
            Workflows to run.
        overwrite_params : Dict, optional
            Workflow parameters to overwrite (applies to all passed workflows), by default None

        Returns
        -------
        Dict
            Dictionary including each workflow together with its run status.
        """

        self.logger.info(f"Checking for pending workflows...")

        if overwrite_params:
            self.logger.debug(f"Overwriting workflow parameters: {overwrite_params}")
            for workflow in workflows:
                self.overwrite_params(workflow, params=overwrite_params)
        
        client = Client("10.125.68.52:8786")

        for workflow in workflows:
            if self.should_run(workflow):
                self.logger.info(f"Worfklow {workflow.name} has been enqueued...")
                future = workflow.submit(client)
        else:
            self.logger.info(f"No pending workflows found")
            client.close()
        return {workflow.name: workflow.status for workflow in workflows}


class SimpleGraph:
    """Produces a simplified Dask graph"""

    def __init__(self, format="png", filename=None):
        self.format = format
        self.filename = filename

    @staticmethod
    def _node_key(s):
        if isinstance(s, tuple):
            return s[0]
        return str(s)

    def visualize(self, x, filename="simple_computation_graph", format=None):

        if hasattr(x, "dask"):
            dsk = x.__dask_optimize__(x.dask, x.__dask_keys__())
        else:
            dsk = x

        deps = {k: get_dependencies(dsk, k) for k in dsk}

        g = graphviz.Digraph(araph_attr={"rankdir": "LR"})

        nodes = set()
        edges = set()
        for k in dsk:
            key = self._node_key(k)
            if key not in nodes:
                g.node(key, label=key_split(k), shape="rectangle")
                nodes.add(key)
            for dep in deps[k]:
                dep_key = self._node_key(dep)
                if dep_key not in nodes:
                    g.node(dep_key, label=key_split(dep), shape="rectangle")
                    nodes.add(dep_key)
                # Avoid circular references
                if dep_key != key and (dep_key, key) not in edges:
                    g.edge(dep_key, key)
                    edges.add((dep_key, key))

        data = g.pipe(format=self.format)
        display_cls = _get_display_cls(self.format)

        if self.filename is None:
            return display_cls(data=data)

        full_filename = ".".join([filename, self.format])
        with open(full_filename, "wb") as f:
            f.write(data)

        return display_cls(filename=full_filename)


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


@retry(Exception, tries=5, delay=5)
def get_con(engine):
    con = engine.connect().connection
    return con


def execute_query(engine, query):
    conn = engine.connect().connection
    cursor = conn.cursor()
    cursor.execute(query)
    conn.commit()
    cursor.close()
    conn.close()
    return None


def update_queue(client, engine, schema=None, table=None):
    tasks = client.cluster.scheduler.tasks
    for task_name in tasks:
        if task_name.endswith("_graph"):
            task = tasks[task_name]
            workflow_name = task_name[: -len("_graph")]
            workflow_status = task.state
            if workflow_status == "memory":  # = finished
                query = f"""
                DELETE FROM {schema}.{table}
                WHERE workflow_name = '{workflow_name}';
                """
                print(query)
                execute_query(engine=engine, query=query)
    return None

