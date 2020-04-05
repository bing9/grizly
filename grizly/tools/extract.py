import csv
import pandas as pd
import openpyxl
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool

# Rename to Extract and remove existing Extract class
class Extract:
    def __init__(self):
        self.df = None
        self.path = None

    def to_csv(self, csv_path, sep="\t", chunksize=None, debug=False, cursor=None):
        if self.tool_name == "QFrame":
            self.sql = self.get_sql()
            if "denodo" in self.engine.lower():
                self.sql += " CONTEXT('swap' = 'ON', 'swapsize' = '500', 'i18n' = 'us_est', 'queryTimeout' = '9000000000', 'simplify' = 'off')"
            row_count = to_csv(
                columns=self.get_fields(aliased=True),
                csv_path=csv_path,
                sql=self.sql,
                engine=self.engine,
                sep=sep,
                chunksize=chunksize,
                cursor=cursor,
            )
            if debug:
                return row_count
            return self
        elif self.tool_name == "GitHub":
            self.df.to_csv(csv_path)

    def to_parquet(self, parquet_path, chunksize=None, debug=False, cursor=None):
        """Saves data to Parquet file.
        TO CHECK: I don't think we need chunksize anymore since we do chunks with
        sql

        Note: You need to use BIGINT and not INTEGER as custom_type in QFrame. The
        problem is that parquet files use int64 and INTEGER is only int4

        Parameters
        ----------
        parquet_path : str
            Path to template Parquet file
        chunksize : str
            Not implemented
        debug : str, optional
            Not implemented
        cursor : int, optional
            Not implemented
        Returns
        -------
        Class
        """
        if self.tool_name == "QFrame":
            self.create_sql_blocks()
            self.sql = self.get_sql()
            con = create_engine(self.engine, encoding="utf8", poolclass=NullPool)
            self.df = pd.read_sql(sql=self.sql, con=con)
            self.df.astype(dtype=self.dtypes).to_parquet(parquet_path)
        elif self.tool_name == "GitHub":
            self.df.astype(dtype=self.df.dtypes).to_parquet(parquet_path)

    def to_excel(
        self,
        input_excel_path,
        output_excel_path,
        sheet_name="",
        startrow=0,
        startcol=0,
        index=False,
        header=False,
    ):
        """Saves data to Excel file.

        Parameters
        ----------
        input_excel_path : str
            Path to template Excel file
        output_excel_path : str
            Path to Excel file in which we want to save data
        sheet_name : str, optional
            Sheet name, by default ''
        startrow : int, optional
            Upper left cell row to dump data, by default 0
        startcol : int, optional
            Upper left cell column to dump data, by default 0
        index : bool, optional
            Write row index, by default False
        header : bool, optional
            Write header, by default False

        Returns
        -------
        Class
        """
        copy_df_to_excel(
            df=self.df,
            input_excel_path=input_excel_path,
            output_excel_path=output_excel_path,
            sheet_name=sheet_name,
            startrow=startrow,
            startcol=startcol,
            index=index,
            header=header,
        )


def copy_df_to_excel(
    df,
    input_excel_path,
    output_excel_path,
    sheet_name="",
    startrow=0,
    startcol=0,
    index=False,
    header=False,
):
    writer = pd.ExcelWriter(input_excel_path, engine="openpyxl")
    book = openpyxl.load_workbook(input_excel_path)
    writer.book = book

    writer.sheets = dict((ws.title, ws) for ws in book.worksheets)

    df.to_excel(
        writer,
        sheet_name=sheet_name,
        startrow=startrow,
        startcol=startcol,
        index=index,
        header=header,
    )

    writer.path = output_excel_path
    writer.save()
    writer.close()


def to_csv(
    columns,
    csv_path,
    sql,
    engine=None,
    sep="\t",
    chunksize=None,
    debug=False,
    cursor=None,
):
    """
    Writes table to csv file.
    Parameters
    ----------
    csv_path : string
        Path to csv file.
    sql : string
        SQL query.
    engine : str, optional
        Engine string. Required if cursor is not provided.
    sep : string, default '\t'
        Separtor/delimiter in csv file.
    chunksize : int, default None
        If specified, return an iterator where chunksize is the number of rows to include in each chunk.
    cursor : Cursor, optional
        The cursor to be used to execute the SQL, by default None
    """
    if cursor:
        cursor.execute(sql)
        close_cursor = False

    else:
        engine = create_engine(engine, encoding="utf8", poolclass=NullPool)

        try:
            con = engine.connect().connection
            cursor = con.cursor()
            cursor.execute(sql)
        except:
            try:
                con = engine.connect().connection
                cursor = con.cursor()
                cursor.execute(sql)
            except:
                raise

        close_cursor = True

    with open(csv_path, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile, delimiter=sep)
        writer.writerow(columns)
        cursor_row_count = 0
        if isinstance(chunksize, int):
            if chunksize == 1:
                while True:
                    row = cursor.fetchone()
                    cursor_row_count += 1
                    if not row:
                        break
                    writer.writerow(row)
            else:
                while True:
                    rows = cursor.fetchmany(chunksize)
                    cursor_row_count += len(rows)
                    if not rows:
                        break
                    writer.writerows(rows)
        else:
            writer.writerows(cursor.fetchall())

    if close_cursor:
        cursor.close()
        con.close()

    return cursor_row_count
