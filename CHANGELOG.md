# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/) 
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html)

## [0.3.5]
### QFrame
- Added `to_records()` method [#417](https://github.com/kfk/grizly/issues/417)
- Added `pivot()` method [#420](https://github.com/kfk/grizly/issues/420)
- Added option to remove fields by alias in `remove()` [#205](https://github.com/kfk/grizly/issues/205)

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
- `get_columns()` - added char and numeric precision while retriving types from redshift


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
- Crosstab (used to bulid crosstab tables)
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

### Deprecated functions (WILL BE ROMOVED IN 0.4)

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
Please check the new structure after dowloading 0.3 grizly to your local folder or do
`from grizly import [module1], [module2], ...`
- **CONFIGURATION**
For the S3 we use AWS configuration so if you don't have it in `.aws` folder please add it. Also for parquet files you need iam_role specified in `.aws/credentials` file. Config class deals with other configuration (for example Email configuration) so in each workflow you have to specify Config first (check docs).
- **PROXY**
You can get some connection errors if you don't have at least one of `HTTPS_PROXY` or `HTTP_PROXY` specified in env variables. Some libraries may not be installed if you don't have `HTTPS_PROXY` specified.


[0.3.5]: https://github.com/kfk/grizly/compare/v0.3.4...0.3.5
[0.3.4]: https://github.com/kfk/grizly/compare/v0.3.3...v0.3.4
[0.3.3]: https://github.com/kfk/grizly/compare/v0.3.2...v0.3.3
[0.3.2]: https://github.com/kfk/grizly/compare/v0.3.1...v0.3.2
[0.3.1]: https://github.com/kfk/grizly/compare/v0.3...v0.3.1
[0.3]: https://github.com/kfk/grizly/compare/v0.2...v0.3
