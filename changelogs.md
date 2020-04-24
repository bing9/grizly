# 0.3.1 to 0.3.2

### SQLDB:
- Added parameter `logger` 
- Added parameter `interface` with options: "sqlalchemy", "turbodbc", "pyodbc"
- check_if_exists() - added option `column`

### S3:
- Added parameter `interface`
- to_rds() - works now with `.parquet` files
- Changed - when skipping upload, `s3.status` is now 'skipped' rather than 'failed'
- Changed - when skipping upload in `s3.from_file()` due to `time_window`, subsequent `self.to_rds()` is also not executed by default
- Added `execute_on_skip` parameter to `to_rds()` to allow overriding above behavior

### QFrame:
- Added parameter `interface`
- to_parquet() - fixed bugs
- copy() - logger is now copied as well
#### new methods
- to_arrow() - writes QFrame records to pyarrow table
- offset() - adds OFFSET statement
- cut() - divides a QFrame into multiple smaller QFrames, each containing chunksize rows
- window() - sorts records and adds LIMIT and OFFSET parameters to QFrame, creating a chunk

