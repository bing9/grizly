import logging
from datetime import datetime

from ..config import Config
from ..tools.sqldb import SQLDB


class JobTable:
    def __init__(
        self, dsn: str = None, schema: str = None, config_key: str = None, logger: logging.Logger = None, **kwargs,
    ):
        self.config = Config().get_service(config_key=config_key, service="schedule")
        dsn = dsn or self.config.get("dsn")
        self.sqldb = SQLDB(dsn=dsn, **kwargs)
        self.schema = schema or self.config.get("schema")
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
                values.append(value)
        columns = ", ".join(columns)
        sql = f"""INSERT INTO {self.full_name} ({columns})
                VALUES {tuple(values)};COMMIT;"""
        name = kwargs.get("name")
        try:
            con.execute(sql)
            self.logger.info(f"Successfully registered job {name} in {self.full_name}")
        except:
            self.logger.exception(f"Error occured during registering job {name} in {self.full_name}")
            self.logger.exception(sql)
            raise
        finally:
            con.close()


class JobRegistryTable(JobTable):
    def __init__(
        self, name: str = None, **kwargs,
    ):
        super().__init__(**kwargs)
        self.name = name or self.config.get("job_registry_table")

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
                    ,source VARCHAR(100) NOT NULL
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

    def register(self, job):
        if not self.exists:
            self.create()

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

    def __register_system_jobs(self):
        self.insert(
            name="System Scheduler",
            owner="SYSTEM",
            type="SCHEDULE",
            source="https://github.com/kfk/grizly/tree/master/grizly/scheduling/script.py",
            source_type="github",
            created_at=datetime.today().__str__(),
        )


class JobStatusTable(JobTable):
    def __init__(
        self, name: str = None, registry_table_name: str = None, **kwargs,
    ):
        super().__init__(**kwargs)
        self.name = name or self.config.get("job_status_table")
        self.registry_table_full_name = JobRegistryTable(name=registry_table_name, **kwargs).full_name

    def create(self):
        """Creates status table"""
        con = self.sqldb.get_connection()

        # TODO: below should be done with SQLDB.create_table but we need table options
        sql = f"""CREATE TABLE {self.full_name} (
                id SERIAL NOT NULL,
                name VARCHAR(50),
                job_id INT NOT NULL ,
                job_name VARCHAR(50) NOT NULL,
                run_date TIMESTAMP (6) NOT NULL,
                run_time INTEGER,
                status VARCHAR(20) NOT NULL,
                env VARCHAR(25),
                PRIMARY KEY(id),
                FOREIGN KEY (job_id) REFERENCES {self.registry_table_full_name}(id)
            );
            """
        try:
            con.execute(sql)
            self.logger.info(f"{self.full_name} has been created successfully")
        except:
            self.logger.exception(f"Error occured during creating table {self.full_name}")
            raise
        finally:
            con.close()
