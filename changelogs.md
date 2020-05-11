# 0.3.2 to 0.3.3
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


## New classes and functions
- `Page`
- `Layout`
- `FinanceLayout`
- `GridLayout`
- `GridCardItem`
- `Text`
- `Row`
- `Column`
