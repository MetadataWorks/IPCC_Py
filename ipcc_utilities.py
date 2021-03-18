# Heiko Maerz MetadataWorks
# heiko@metadataworks.co.uk
# IPCC Utilities

# global packages
import copy
import datetime
import json
import os
import pandas as pd
import requests
import platform
import mysql.connector

# local packages
import db_config

# global variables
CWD = os.getcwd()
__version__ = '20210318_0814'


class Hooman:
    def __init__(self, cmax, dout, text):
        self.total = cmax
        self.pbar_length = dout
        self.curr = 0
        self.count = 0
        self.text = text

    def print_progress(self):
        self.count += 1
        progress = (int(self.count * (self.pbar_length - 1) / self.total) + 1)
        if progress > self.curr:
            pbar = progress * '*'
            pout = f"\r {self.text} >{pbar.ljust(self.pbar_length, ' ')}<"
            print(pout, end="")
            self.curr = progress
        return


def write_header():
    write_timestamp(f"{__file__}=={__version__}")
    print(f"python=={platform.python_version()}")
    print(f"json=={json.__version__}")
    print(f"mysql.connector=={mysql.connector.__version__}")
    print(f"pandas=={pd.__version__}")
    print(f"requests=={requests.__version__}")
    print(f"{os.path.join(CWD, 'db_config.py')}=={db_config.__version__}@{db_config.__db_id__}")
    print()
    return


def connect_to_db(db_logon):
    db = None
    try:
        db = mysql.connector.connect(**db_logon)
    except mysql.connector.Error as err:
        if err.errno == mysql.connector.errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
            return None, None
        elif err.errno == mysql.connector.errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
            print()
            return None, None
        else:
            print(err)
            print()
            return None, None
    return db


def get_json(json_uri):
    if isinstance(json_uri, dict):
        return json_uri
    elif os.path.isfile(json_uri):
        with open(json_uri, 'r') as json_file:
            return json.load(json_file)
    elif json_uri.startswith('http'):
        return requests.get(json_uri).json()
    else:
        raise Exception


def export_json(data, filename, indent=2):
    with open(filename, 'w') as jsonfile:
        json.dump(data, jsonfile, indent=indent)
    return


def ldict_to_dlists(ldict, cols, fill_na=''):
    dlists = {col: [] for col in cols}
    for din in ldict:
        for col in cols:
            dlists[col].append(din.get(col, fill_na))
    return dlists


def write_excel(fname, worksheets, idx=False):
    with pd.ExcelWriter(fname) as writer:
        for sheetname, df_worksheet in worksheets.items():
            df_worksheet.to_excel(writer, sheet_name=sheetname, index=idx)
    return


def sql_select_to_pd(db, sql_statement):
    sql_select = None
    try:
        sql_select = pd.read_sql(sql_statement, con=db)
    except Exception as e:
        write_timestamp(f"SQL select error {e}")

    return sql_select


def sql_select_to_json(db, sql_statement):
    json_select = None
    df_select = sql_select_to_pd(db, sql_statement)
    try:
        json_select = df_select.to_dict(orient='records')
    except Exception as e:
        write_timestamp(f"{e}")
    return json_select


def write_timestamp(out_text='', verbose = True):
    if not verbose:
        return
    now = datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S")
    print(f"{now} {out_text}")
    return


def main():
    write_header()

    write_timestamp(f"done")
    return


if '__main__'==__name__:
    main()