import csv
import pandas as pd
import openpyxl
from .sqldb import SQLDB
import logging
from os.path import basename


class BaseTool:
    def __init__(
        self, chunksize: int = None, logger: logging.Logger = None, debug: bool = False, config_key: str = "standard",
    ):
        """TODO: Probably we don't need tool_name, df, dtypes and path"""
        self.tool_name = self.__class__.__name__
        self.df = None
        self.dtypes = None
        self.path = None
        self.chunksize = chunksize
        self.logger = logger or logging.getLogger(__name__)
        self.debug = debug
        self.config_key = config_key

    def to_csv(self, csv_path, sep="\t", chunksize=None, debug=False):

        if self.__class__.__name__ == "QFrame":
            self.sql = self.get_sql()
            if "denodo" in self.engine.lower() and self.sqldb.db == "denodo":
                self.sql += " CONTEXT('swap' = 'ON', 'swapsize' = '400', 'i18n' = 'us_est', 'queryTimeout' = '9000000000', 'simplify' = 'off')"

            self.logger.info(f"Downloading data into '{basename(csv_path)}'...")
            row_count = to_csv(
                columns=self.get_fields(aliased=True, not_selected=False),
                csv_path=csv_path,
                sql=self.sql,
                sqldb=self.sqldb,
                sep=sep,
                chunksize=chunksize,
            )
            self.logger.info(f"Successfully wrote to '{basename(csv_path)}'")
            if debug:
                return row_count
            return self
        elif self.__class__.__name__ == "GitHub":
            self.logger.info(f"Downloading data into '{basename(csv_path)}'...")
            self.df.to_csv(csv_path)
        else:
            raise NotImplementedError(f"This method is not supported for {self.__class__.__name__} class.")

    def to_parquet(self, parquet_path, debug=False):
        """Saves data to Parquet file.
        TO CHECK: I don't think we need chunksize anymore since we do chunks with
        sql

        Note: You need to use BIGINT and not INTEGER as custom_type in QFrame. The
        problem is that parquet files use int64 and INTEGER is only int4


        Parameters
        ----------
        parquet_path : str
            Path to template Parquet file
        debug : str, optional
            Whether to display the number of rows returned by the query
        Returns
        -------
        Class
        """
        if self.__class__.__name__ == "QFrame":
            self.df = self.to_df()
            if not self.df.empty:
                dtypes = {}
                qf_dtypes = self.get_dtypes()
                qf_fields = self.get_fields(aliased=True)
                for col in self.df.columns:
                    if col in qf_fields:
                        _type = qf_dtypes[qf_fields.index(col)]
                        dtype = "object"
                        if _type == "num":
                            dtype = "float64"
                        elif _type == "dim":
                            dtype == "object"
                        dtypes[col] = dtype
                self.df.astype(dtype=dtypes).to_parquet(parquet_path)
        elif self.__class__.__name__ == "GitHub":
            self.df.astype(dtype=self.df.dtypes).to_parquet(parquet_path)
        else:
            raise NotImplementedError(f"This method is not supported for {self.__class__.__name__} class.")
        if debug:
            return self.df.shape[0] or 0

    def to_excel(
        self, input_excel_path, output_excel_path, sheet_name="", startrow=0, startcol=0, index=False, header=False,
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
        if self.__class__.__name__ == "QFrame":
            df = self.to_df()
        elif self.__class__.__name__ == "GitHub":
            df = self.df
        else:
            raise NotImplementedError(f"This method is not supported for {self.__class__.__name__} class.")

        copy_df_to_excel(
            df=df,
            input_excel_path=input_excel_path,
            output_excel_path=output_excel_path,
            sheet_name=sheet_name,
            startrow=startrow,
            startcol=startcol,
            index=index,
            header=header,
        )


def copy_df_to_excel(
    df, input_excel_path, output_excel_path, sheet_name="", startrow=0, startcol=0, index=False, header=False,
):
    writer = pd.ExcelWriter(input_excel_path, engine="openpyxl")
    book = openpyxl.load_workbook(input_excel_path)
    writer.book = book

    writer.sheets = dict((ws.title, ws) for ws in book.worksheets)

    df.to_excel(
        writer, sheet_name=sheet_name, startrow=startrow, startcol=startcol, index=index, header=header,
    )

    writer.path = output_excel_path
    writer.save()
    writer.close()


def to_csv(columns, csv_path, sql, sqldb, sep="\t", chunksize=None, debug=False):
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
    """
    con = sqldb.get_connection()
    cursor = con.cursor()
    cursor.execute(sql)

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
            rows = cursor.fetchall()
            cursor_row_count += len(rows)
            writer.writerows(rows)

    cursor.close()
    con.close()

    return cursor_row_count
