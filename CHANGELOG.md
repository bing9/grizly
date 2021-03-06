# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html)

## [0.4.2](https://github.com/kfk/grizly/compare/v0.4.1...v0.4.2) - 18-12-2020

### Job

- Deprecated `Job.tasks` and added `Job.func` instead [#660](https://github.com/tedcs/grizly/issues/660). Job can store any function not only dask tasks and it can also store parameters.

    ```python
    def my_func(a, b):
        return a + b
    
    Job("my_job_name").register(func, 3, b=2, if_exists="replace")
    ```
- Added `Job.logs` [#506](https://github.com/tedcs/grizly/issues/506) and `Job.traceback`

### Extract
- Added automatic validations
- Added `run()` method as a thin wrapper around `Job.submit()`
- It's now preferred to run `Extract.register()` and then `Extract.run()` rather than `Extract.submit()`,
which was confusing to use and to code, as well as inefficient

### QFrame

- Added possibility to read source information from json

## [0.4.1](https://github.com/kfk/grizly/compare/v0.4...v0.4.1) - 01-12-2020

### Overall

- Rewritten the `Extract` tutorial to adapt it to grizly v0.4

### Config
- `address` key was changed to `auth_address`, and `send_as` is now simply `address`.
This makes it more explicit that you can authenticate using one address, and use another mailbox
with these credentials (eg. to send as another email address)

### QFrame
- Fixed bugs [#446](https://github.com/tedcs/grizly/issues/446), [#505](https://github.com/tedcs/grizly/issues/505) and [#653](https://github.com/tedcs/grizly/issues/653)

### git s3

- `pull` option works now with the number (position in track list) [#646](https://github.com/tedcs/grizly/issues/646)
- Fixed `synced` option [#647](https://github.com/tedcs/grizly/issues/647)

## [0.4](https://github.com/kfk/grizly/compare/v0.4.0rc0...v0.4) - 18-11-2020

### Overall changes

The documentation has been cleaned up and the bugs from previous release candidate has been fixed.

### git s3

- Added first version of git s3 cli https://github.com/tedcs/grizly/issues/517

### Scheduling

- Added option to run a Job on upstream job `result_change`, `success` or `fail` https://github.com/tedcs/grizly/issues/500

## [0.4.0rc0](https://github.com/kfk/grizly/compare/v0.3.8...v0.4.0rc0) - 26-10-2020

### Overall changes

This release contains a lot of internal api changes. We extended QFrame to support also Salesforce database and we are planning to extend it to be able to query filesystems and APIs. To make your life easier we want to manage sources configuration in grizly config file so that you only need to pass dsn (data source name). We also moved to new scheduling infrastructure so **old orchestrate has been removed**.

### QFrame

- Added `_fix_types()`, which changes the data types inside QFrame (self.data)
based on what types are actually retrieved from the top 100 rows
- Added QFrame.store with `to_dict()` and `to_json()` methods
- Added SFDC driver - requires right configuration file and then can be used like
    ```python
    from grizly import QFrame

    qf = QFrame(dsn="sfdc", table="Account")
    qf.remove_compound_fields() # querying compound fields is not supported
    qf.limit(10).to_df()
    ```
- Added `to_crosstab()`, `to_arrow()` methods
- Deprecated `save_json()` method (`QFrame.store.to_json()` should be used instead)
- Replaced 'type' and 'custom_type' keys with 'dtype' key

### Config

- Changed `email_address` and `email_password` keys to `address` and `password`
- Renamed `sqldb` key to `sources` key and moved `github` and `sfdc` keys under

### Extract

- Added SimpleExtract to extract data in single-node mode
- Added SFDCExtract for Salesforce extracts

### Removed classes

- Workflow, Listener, EmailListener, Schedule, Runner - please use Job class to manage scheduling instead
- Store

### Removed functions

- QFrame.to_rds
- QFrame.csv_to_s3
- QFrame.s3_to_rds
- s3_to_csv
- csv_to_s3
- df_to_s3
- s3_to_rds
- check_if_exists
- create_table
- write_to
- get_columns
- delete_where
- copy_table
- read_config
- initiate


## [0.3.8](https://github.com/kfk/grizly/compare/v0.3.7...v0.3.8) - 17-09-2020

### Job

- Added properties: `description`, `time_now`

### JobRun

- Added properties: `result`

### Config

- Added `s3` key with `bucket` subkey

### Extract

- Released first version
- Moved from `dangerous/experimental.py` to `tools/extract.py`
- Added tutorial in `tutorials`

## [0.3.7](https://github.com/kfk/grizly/compare/v0.3.7rc1...v0.3.7) - 04-09-2020

### Scheduling
- Released first version with option to schedule jobs using crons and upstream jobs 
    - [api](https://github.com/tedcs/grizly/issues/512)
    - example
      ```python
      @dask.delayed
      def add(x, y):
          return x + y

      sum_task_1 = add(1, 2)
      job = Job(name="job_name_1")
      job.register(tasks=[sum_task_1],
                   owner="johnsnow@example.com",
                   crons="* * * * *")

      sum_task_2 = add(2, 3)
      job = Job(name="job_name_2")
      job.register(tasks=[sum_task_2],
                   owner="johnsnow@example.com",
                   upstream="job_name_1")
      ```

### Docker
- Integrated platform with grizly repo

### S3
- Adjusted `to_df()` and `from_df()` methods to load data to memory not local files  [#524](https://github.com/tedcs/grizly/issues/524)

## [0.3.7rc1](https://github.com/kfk/grizly/compare/v0.3.7rc0...v0.3.7rc1) - 13-08-2020

### QFrame
- Fixed bug in `having` method [#472](https://github.com/acivitillo/grizly/issues/472)

## [0.3.7rc0](https://github.com/kfk/grizly/compare/v0.3.6...v0.3.7rc0) - 22-07-2020

### Overall changes
- This release was done for clean up the repo.

## [0.3.6] - 10-07-2020

### Overall changes

- Removed package PageLayout
- Added the possibility to run and cancel control checks (eg. `grizly workflow run "sales daily news control check" --local`). To cancel checks running on prod, run eg. `grizly workflow cancel "sales daily news control check"`
- **IMPORTANT**: Engine strings (`engine` or `engine_str` parameters) are deprecated since version `0.3.6`. They are replaced with suitable data source names (`dsn` parameter) [#455](https://github.com/kfk/grizly/issues/455)
- Added experimental.py with the experimental Extract class
- Removed SQLAlchemy from requirements

### Orchestrate

- Changed `workflows_dir` to use the current `GRIZLY_WORKFLOWS_HOME`

### Workflow

- **IMPORTANT** run() method has been removed

### SQLDB

- Changed configuration. Now the main parameter is `dsn` not `db` [#459](https://github.com/kfk/grizly/issues/459)
- Added `get_tables()` method
- Added option `type='external'` to `create_table()` method [#451](https://github.com/kfk/grizly/issues/451)

### S3

- Added `to_aurora()` method [#404](https://github.com/kfk/grizly/issues/404)
- Added `to_json()` method
- Added `from_serializable()` method

### QFrame

- Improved SQL generation [#462](https://github.com/kfk/grizly/issues/462), [#430](https://github.com/kfk/grizly/issues/430)
- Added option to use fields aliases in methods: `rename()`, `agg()` [#445](https://github.com/kfk/grizly/issues/445)
- Improved `from_table()` method to work with external tables [#442](https://github.com/kfk/grizly/issues/442)


## [0.3.5] - 02-06-2020

### Overall changes

- Added a basic CLI from submitting and cancelling workflows: `grizly workflow run wf_name` and `grizly workflow cancel wf_name`

### QFrame

- Added `to_records()` method [#417](https://github.com/kfk/grizly/issues/417)
- Added `pivot()` method [#420](https://github.com/kfk/grizly/issues/420)
- Added option to use fields aliases in methods: `rearrange()`, `select()`, `orderby()`, `groupby()`, `remove()` [#205](https://github.com/kfk/grizly/issues/205)
- Changed all prints (except the few necessary ones) to logs

### Crosstab

- Added method `hide_columns()` (works for now only for dimensions)

## [0.3.4] - 21-05-2020

### S3

- Fixed automatic column order in `to_rds()` method. [#377](https://github.com/kfk/grizly/issues/377)
- Added `redshift_str` parameter to `to_rds()` method - it shouldn't be specified on initiating `S3` class.

### Orchestrate

- Added wf.cancel() for convenient workflow cleanup from the scheduler

### QFrame

- Added `order_by` parameter to `window()` and `cut()` methods
- Added `from_table()` method [#416](https://github.com/kfk/grizly/issues/416)
- Deprecated `read_dict()` method
- Added `from_dict()` method
- Added `db` parameter
- `create_table()` method is not deprecated now [#411](https://github.com/kfk/grizly/issues/411) 

### Crosstab

- Added indented view [#422](https://github.com/kfk/grizly/issues/422)

## [0.3.3] - 08-05-2020

### Overall changes

- Requirements are not installed now automatically because this process was breaking in Linux environment. After pulling the latest version you have to install requirements and then install grizly.
- Added option `pip install grizly` but it works only if you have pulled grizly - it WON'T do it for you.
- For more info about these changes check `README.md`

### QFrame

- `cut()` - fixed bug with omitting first row [#408](https://github.com/kfk/grizly/issues/408)
- `copy()` - interface is now copied as well
- `to_csv()` - interface parameter is now used if specified
- Added possibility to check what is row count of generated SQL by doing `len(qf)`

### SQLDB

- `get_columns()` - added char and numeric precision while retrieving types from redshift

### New classes and functions

- `Page`
- `Layout`
- `FinanceLayout`
- `GridLayout`
- `GridCardItem`
- `Text`
- `Row`
- `Column`

## [0.3.2] - 24-04-2020

### SQLDB:

- Added parameter `logger`
- Added parameter `interface` with options: "sqlalchemy", "turbodbc", "pyodbc"
- `check_if_exists()` - added option `column`

### S3:

- Added parameter `interface`
- `to_rds()` - works now with `.parquet` files
- Changed - when skipping upload, `s3.status` is now 'skipped' rather than 'failed'
- Changed - when skipping upload in `s3.from_file()` due to `time_window`, subsequent `self.to_rds()` is also not executed by default
- Added `execute_on_skip` parameter to `to_rds()` to allow overriding above behavior

### QFrame:

- Added parameter `interface`
- `to_parquet()` - fixed bugs
- `copy()` - logger is now copied as well

#### new methods

- `to_arrow()` - writes QFrame records to pyarrow table
- `offset()` - adds OFFSET statement
- `cut()` - divides a QFrame into multiple smaller QFrames, each containing chunksize rows
- `window()` - sorts records and adds LIMIT and OFFSET parameters to QFrame, creating a chunk


## [0.3.1] - 31-03-2020

### Overall changes:

This release contains mostly some bug fixes like hard-coded paths.

### Github:

- from_issues() - remove `org_name` parameter, added `url` parameter

## [0.3] - 27-03-2020

### Overall changes:

- Removed `bulk/` prefix from all functions which uses s3
- Set default engine string `mssql+pyodbc://redshift_acoe` for redshift and default s3 bucket to `acoe-s3`

### QFrame:

- Changed get_sql() output, now it returns SQL string not QFrame
- To print sql saved in QFrame you can do `print(qf)`

### New classes and functions

- SFDC
- Github
- S3 (replaced AWS)
- Config (used to set configuration)
- Crosstab (used to build crosstab tables)
- SQLDB (contains all functions which interacts with databases)

### Removed classes and functions

#### Published

- Excel
- AWS (moved to S3 class, names of the methods also changed)
- Extract (**WARNING!!**: This class still exists but has completely different attributes and methods)
- Load
- to_s3
- read_s3
- s3_to_rds_qf
- QFrame.read_excel
- Store.get_redshift_columns

#### Not published

- initialize_logging
- get_last_working_day
- get_denodo_columns (moved to SQLDB method)
- get_redshift_columns (moved to SQLDB method)

### Deprecated functions (WILL BE REMOVED IN 0.4)

** old function -> what should be used instead **

- QFrame.create_table -> SQLDB.create_table
- QFrame.to_rds -> QFrame.to_csv, S3.from_file and S3.to_rds
- QFrame.csv_to_s3 -> S3.from_file
- QFrame.s3_to_rds -> S3.to_rds
- s3_to_csv -> S3.to_file
- csv_to_s3 -> S3.from_file
- df_to_s3 -> S3.from_df and S3.to_rds
- s3_to_rds -> S3.to_rds
- check_if_exists -> SQLDB.check_if_exists
- create_table -> SQLDB.create_table
- write_to -> SQLDB.write_to
- get_columns -> SQLDB.get_columns
- delete_where -> SQLDB.delete_where
- copy_table -> SQLDB.copy_table
- read_config -> Config().from_json function or in case of AWS credentials - start using S3 class !!!

### What can go wrong?

- **IMPORTING MODULES**
We changed folder structure. This may affect those of you who does for example 
`from grizly.orchestrate import ...`
Please check the new structure after downloading 0.3 grizly to your local folder or do
`from grizly import [module1], [module2], ...`
- **CONFIGURATION**
For the S3 we use AWS configuration so if you don't have it in `.aws` folder please add it. Also for parquet files you need iam_role specified in `.aws/credentials` file. Config class deals with other configuration (for example Email configuration) so in each workflow you have to specify Config first (check docs).
- **PROXY**
You can get some connection errors if you don't have at least one of `HTTPS_PROXY` or `HTTP_PROXY` specified in env variables. Some libraries may not be installed if you don't have `HTTPS_PROXY` specified.

[0.3.8]: https://github.com/kfk/grizly/compare/v0.3.7...v0.3.8
[0.3.7]: https://github.com/kfk/grizly/compare/v0.3.6...v0.3.7
[0.3.6]: https://github.com/kfk/grizly/compare/v0.3.5...v0.3.6
[0.3.5]: https://github.com/kfk/grizly/compare/v0.3.4...v0.3.5
[0.3.4]: https://github.com/kfk/grizly/compare/v0.3.3...v0.3.4
[0.3.3]: https://github.com/kfk/grizly/compare/v0.3.2...v0.3.3
[0.3.2]: https://github.com/kfk/grizly/compare/v0.3.1...v0.3.2
[0.3.1]: https://github.com/kfk/grizly/compare/v0.3...v0.3.1
[0.3]: https://github.com/kfk/grizly/compare/v0.2...v0.3
