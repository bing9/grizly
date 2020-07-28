from datetime import datetime, timedelta, timezone
import click
from dask.distributed import Future
from distributed import Client
from distributed.diagnostics.plugin import SchedulerPlugin
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from grizly import Config, Email
from grizly.utils import get_path
import logging
import sys
import csv

sys.path.insert(0, "/home/acoe_workflows")

# config_path = get_path(".grizly", "config.json")
# Config().from_json(config_path)

children_queue_path = "/home/acoe_workflows/workflows/etc/children_queue.txt"


class UpdateState(SchedulerPlugin):
    def __init__(self, scheduler, redshift_str=None):
        self.scheduler = scheduler
        self.engine_str = redshift_str or "mssql+pyodbc://redshift_acoe"
        self.engine = create_engine(self.engine_str, encoding="utf8", poolclass=NullPool)
        self.schema = "administration"
        self.status_table = "status"
        self.metadata_table = "workflow"
        self.queue_table = "workflow_queue"
        self.logger = logging.getLogger("distributed.scheduler").getChild("update_state")
        self.start_times = {}

    def generate_notification(self, workflow_name, status, start_time, duration):
        start_time = start_time.strftime("%b %d, %H:%M:%S")
        subject = f"Workflow {workflow_name} finished with the status {status}"
        email_body = f"Carefully crafted workflow {workflow_name} scheduled on {start_time} finished in {duration} seconds with the status {status}"
        notification = Email(subject=subject, body=email_body, logger=self.logger)
        return notification

    def run_query(self, query):
        con = self.engine.connect().connection
        cursor = con.cursor()
        cursor.execute(query)
        result = None
        query_cleaned = query.replace(" ", "").lower()
        if "select" in query_cleaned[:8]:
            self.logger.debug("Fetching results...")
            result = cursor.fetchall()
            try:
                result = result[0][0]
            except:
                pass
        con.commit()
        cursor.close()
        con.close()
        return result

    @staticmethod
    def add_to_children_queue(children):
        with open(children_queue_path, "a+") as f:
            writer = csv.writer(f, delimiter="\n")
            if isinstance(children, str):
                writer.writerow(children)
            elif isinstance(children, list):
                writer.writerows([children])
            else:
                raise ValueError("Children should be of type str or list")

    def update_graph(self, scheduler, dsk=None, keys=None, restrictions=None, **kwargs):
        workflow_name = keys.pop().replace("_graph", "")
        now = datetime.now(timezone.utc)
        self.start_times[workflow_name] = now

    def transition(self, key, start, finish, *args, **kwargs):
        task_state = self.scheduler.tasks[key]
        exception = task_state.exception
        traceback = task_state.traceback
        timestamp = datetime.now(timezone.utc)
        duration = 0

        if "_graph" in key:
            self.logger.info(f"Workflow {key.replace('_graph', '')} has changed its status from {start} to {finish}")

            status = "processing"
            if (start == "memory") or (finish == "memory"):
                status = "success"
            elif (start == "erred") or (finish == "erred"):
                status = "failure"
            elif (start == "processing") and (finish == "forgotten"):
                client = Client(self.scheduler.address)
                f = Future(key, client=client)
                self.logger.info(
                    f"Removing workflow ({key.replace('_graph', '')}; status: {f.status}) from the scheduler.."
                )
                f.cancel(force=True)
                client.close()
                return
            else:
                return

            workflow_name = key.replace("_graph", "")

            # TODO: do one select instead of 4
            subscribers_query = f"""
            SELECT subscribers
            FROM {self.schema}.{self.metadata_table}
            WHERE name = '{workflow_name}'
            """
            remove_from_queue = f"""
            DELETE FROM {self.schema}.{self.queue_table}
            WHERE workflow_name = '{workflow_name}'
            """
            children_query = f"""
            SELECT children
            FROM {self.schema}.{self.metadata_table}
            WHERE name = '{workflow_name}'
            """
            email_owner_query = f"""
            SELECT email_owner
            FROM {self.schema}.{self.metadata_table}
            WHERE name = '{workflow_name}'
            """
            email_backup_query = f"""
            SELECT email_backup
            FROM {self.schema}.{self.metadata_table}
            WHERE name = '{workflow_name}'
            """

            if status == "success":
                try:
                    children = eval(self.run_query(children_query))
                except TypeError:
                    children = None
                if children:
                    children_modules = []
                    for child in children:
                        child_module = self.run_query(
                            f"SELECT module_name FROM {self.schema}.{self.metadata_table} WHERE name = '{child}'"
                        )
                        children_modules.append(child_module)
                    self.add_to_children_queue(children_modules)
                    self.logger.debug(f"Enqueued children: {', '.join(children)}")

            if status == "failure":
                # clean up
                client = Client(self.scheduler.address)
                f = Future(key, client=client)
                self.logger.info(
                    f"Removing failed workflow ({key.replace('_graph', '')}; status: {f.status}) from the scheduler.."
                )
                f.cancel(force=True)
                client.close()

            start_time = self.start_times.get(workflow_name)
            finish_time = datetime.now(timezone.utc)
            duration = (finish_time - start_time).seconds

            insert_run_metadata = f"""
            INSERT INTO {self.schema}.{self.status_table} (workflow_name, owner_email, backup_email, run_date, workflow_status, run_time, env, error_value, error_type) VALUES (
                '{workflow_name}',
                (SELECT email_owner FROM {self.schema}.{self.metadata_table} WHERE name = '{workflow_name}'),
                (SELECT email_backup FROM {self.schema}.{self.metadata_table} WHERE name = '{workflow_name}'),
                '{timestamp.replace(microsecond=0)}',
                '{status}',
                {duration},
                (SELECT env FROM {self.schema}.{self.metadata_table} WHERE name = '{workflow_name}'),
                '{exception or ""}',
                '{traceback or ""}'
                )
            """

            self.logger.info(f"Workflow {workflow_name} completed with the status {status}")
            self.run_query(remove_from_queue)
            self.logger.info(f"Workflow {workflow_name} has been removed from {self.queue_table}")
            self.run_query(insert_run_metadata)
            self.logger.info(f"Workflow {workflow_name}'s status has been uploaded to {self.status_table}")
            subscribers = eval(self.run_query(subscribers_query))
            email_owner = self.run_query(email_owner_query)
            email_backup = self.run_query(email_backup_query)
            email_to = [email_owner, email_backup] + subscribers
            notification = self.generate_notification(
                workflow_name=workflow_name, status=status, start_time=start_time, duration=duration
            )
            notification.send(to=email_to)
            self.logger.info(f"Workflow {workflow_name}'s status has been sent to: {', '.join(email_to)}")


@click.command()
def dask_setup(scheduler):
    plugin = UpdateState(scheduler=scheduler)
    scheduler.add_plugin(plugin)
