import logging
from datetime import datetime
import json

from ..config import Config
from ..tools.sqldb import SQLDB
from ..tools.qframe import QFrame


class JobTable:
    def __init__(
        self, logger: logging.Logger = None,
    ):
        self.config = Config().get_service(service="schedule")
        self.sqldb = SQLDB(dsn=self.config.get("dsn"))
        self.schema = self.config.get("schema")
        self.logger = logger or logging.getLogger(__name__)
        self.name = ""

    @property
    def full_name(self):
        if self.schema is None or self.schema == "":
            return self.name
        else:
            return f"{self.schema}.{self.name}"

    @property
    def exists(self) -> bool:
        """Checks if table exists"""
        return self.sqldb.check_if_exists(table=self.name, schema=self.schema)

    def insert(self, **kwargs):
        con = self.sqldb.get_connection()
        columns = []
        values = []
        for key, value in kwargs.items():
            if value is not None:
                columns.append(key)
                if isinstance(value, dict):
                    value = json.dumps(value)
                values.append(str(value))
        columns = ", ".join(columns)
        sql = f"""INSERT INTO {self.full_name} ({columns})
                VALUES {tuple(values)};COMMIT;"""
        name = kwargs.get("name") or "status"
        try:
            con.execute(sql)
            self.logger.info(f"Successfully registered job {name} in {self.full_name}")
        except:
            self.logger.exception(f"Error occured during registering job {name} in {self.full_name}")
            self.logger.exception(sql)
            raise
        finally:
            con.close()

    def update(self, id, **kwargs):
        con = self.sqldb.get_connection()
        columns = []
        values = []
        for key, value in kwargs.items():
            if value is not None:
                columns.append(key)
                values.append(str(value))
        items = ", ".join([f"{col}='{val}'" for col, val in zip(columns, values)])
        sql = f"""UPDATE {self.full_name}
                SET {items}
                WHERE id={id};COMMIT;"""
        try:
            con.execute(sql)
            self.logger.info(f"Successfully updated record with id {id} in {self.full_name}")
        except:
            self.logger.exception(f"Error occured during updating record with id {id} in {self.full_name}")
            self.logger.exception(sql)
            raise
        finally:
            con.close()


class JobRegistryTable(JobTable):
    def __init__(
        self, **kwargs,
    ):
        super().__init__(**kwargs)
        self.name = self.config.get("job_registry_table")
        if not self.exists:
            self.create()

    def create(self):
        """Creates registry table"""
        con = self.sqldb.get_connection()

        # TODO: below should be done with SQLDB.create_table but we need table options
        sql = f"""CREATE TABLE {self.full_name} (
                    id SERIAL NOT NULL
                    ,name VARCHAR(50) UNIQUE NOT NULL
                    ,owner VARCHAR(50) NOT NULL
                    ,type VARCHAR(20) DEFAULT NULL
                    ,notification JSONB
                    ,trigger JSONB
                    ,source JSONB NOT NULL
                    ,source_type VARCHAR(20) NOT NULL
                    ,created_at TIMESTAMP (6) NOT NULL
                    ,PRIMARY KEY (id)
                    ,CONSTRAINT check_system CHECK ((
                        owner = 'SYSTEM'
                        AND type = 'SCHEDULE'
                        AND trigger IS NULL
                        )
                    OR (
                        owner != 'SYSTEM'
                        AND type != 'SCHEDULE'
                        AND trigger IS NOT NULL
                        )
                    ));

                COMMIT;
            """
        try:
            con.execute(sql)
            self.logger.info(f"{self.full_name} has been created successfully")
        except:
            self.logger.exception(f"Error occured during creating table {self.full_name}")
            raise
        finally:
            con.close()

        self.__register_system_jobs()
        JobStatusTable(logger=self.logger).create()

    def register(self, job):

        job_id = self._get_job_id(job)
        if job_id is None:
            self.insert(
                name=job.name,
                owner=job.owner,
                trigger=job.trigger,
                notification=job.notification,
                type=job.type,
                source=job.source,
                source_type=job.source_type,
                created_at=datetime.today().__str__(),
            )
            return self._get_job_id(job)
        else:
            self.logger.exception(f"Job {job.name} already exists in {self.full_name}")
            return job_id

    def __register_system_jobs(self):
        self.insert(
            name="System Scheduler",
            owner="SYSTEM",
            type="SCHEDULE",
            source='{"main": "https://github.com/kfk/grizly/tree/master/grizly/scheduling/script.py"}',
            source_type="github",
            created_at=datetime.today().__str__(),
        )

    def _get_job_id(self, job):
        qf = QFrame(sqldb=self.sqldb)
        qf.from_table(table=self.name, schema=self.schema, columns=["id"])
        qf.query(f"name='{job.name}'")
        records = qf.to_records()

        if records:
            return records[0][0]


class JobStatusTable(JobTable):
    def __init__(
        self, **kwargs,
    ):
        super().__init__(**kwargs)
        self.name = self.config.get("job_status_table")
        self.registry_table_full_name = JobRegistryTable(**kwargs).full_name
        if not self.exists:
            self.create()

    def create(self):
        """Creates status table"""
        con = self.sqldb.get_connection()

        # TODO: below should be done with SQLDB.create_table but we need table options
        sql = f"""CREATE TABLE {self.full_name} (
                id SERIAL NOT NULL,
                job_id INT NOT NULL ,
                job_name VARCHAR(50) NOT NULL,
                run_date TIMESTAMP (6) NOT NULL,
                run_time INTEGER,
                status VARCHAR(20) NOT NULL,
                env VARCHAR(25),
                PRIMARY KEY(id),
                FOREIGN KEY (job_id) REFERENCES {self.registry_table_full_name}(id)
            );

            COMMIT;
            """
        try:
            con.execute(sql)
            self.logger.info(f"{self.full_name} has been created successfully")
        except:
            self.logger.exception(f"Error occured during creating table {self.full_name}")
            raise
        finally:
            con.close()

    def register(self, status):

        self.insert(
            job_id=status.job.id,
            job_name=status.job.name,
            status=status.status,
            env=status.job.env,
            run_date=datetime.today().__str__(),
        )

        status_id = self._get_last_status_id(job=status.job)

        return status_id

    def _get_last_status_id(self, job):
        qf = QFrame(sqldb=self.sqldb)
        qf.from_table(table=self.name, schema=self.schema, columns=["id", "run_date"])
        qf.query(f"job_name='{job.name}'")
        qf.orderby("run_date", ascending=False)
        qf.limit(1)
        records = qf.to_records()

        if records:
            return records[0][0]
