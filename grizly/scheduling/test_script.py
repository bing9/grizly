import dask
import logging

from grizly import QFrame, Job, SQLDB, get_path, S3, Workflow

logger=logging.getLogger("distributed.worker.test")


@dask.delayed
def load_qf():
    dsn = get_path("grizly_dev", "tests", "Chinook.sqlite")
    sqldb = SQLDB(dsn=dsn, db="sqlite", dialect="mysql", logger=logger)

    qf = QFrame(sqldb=sqldb, logger=logger).from_table(table="Track")
    qf.rename({field: field.lower() for field in qf.get_fields()})
    
    return qf
    
@dask.delayed
def create_table(qf):
    qf.create_table(dsn="redshift_acoe", table="track", schema="grizly")
    
@dask.delayed
def qf_to_df(qf):
    return qf.to_df()

@dask.delayed
def df_to_s3(df):
    return S3(logger=logger).from_df(df, keep_file=False)
    
@dask.delayed
def s3_to_rds(s3, upstream):
    s3.to_rds(table="track", schema="grizly", dsn="redshift_acoe", if_exists="replace")
    
    
    
qf = load_qf()
tbl = create_table(qf)
df = qf_to_df(qf)
s3 = df_to_s3(df)
rds = s3_to_rds(s3, upstream=tbl)

tasks=[rds]