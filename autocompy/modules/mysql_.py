import os
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text

from autocompy import config
from autocompy import main_webm


def cols(db_sch_tb, specific_cols):
    try:
        connect = create_engine("mysql://root:root@localhost/world")

        if specific_cols[0] in ["*", ""]:
            df1 = pd.read_sql(f'''SELECT * from {db_sch_tb} limit 1''', connect)
        else:
            df1 = pd.read_sql(f'''SELECT {",".join(specific_cols)} from {db_sch_tb} limit 1''', connect)

        return list(df1.columns)

    except Exception as e:
        print("Exception", e)
        with open(os.path.join(main_webm.output_dir, 'errors.txt'), 'a', newline='') as f:
            f.write(f"\n\n--- Exception {datetime.now().replace(microsecond=0)}---\n{e}")
            main_webm.status = "[MySQL Cols] - Something Went Wrong !!"


def dataframe(db_tb, specific_cols):
    try:
        connect = create_engine("mysql://root:root@localhost/world")

        if specific_cols[0] in ["*", ""]:
            df1 = pd.read_sql(f'select * from {db_tb}', connect)
        else:
            df1 = pd.read_sql(f'''select {",".join(specific_cols)} from {db_tb}''', connect)

        with open(os.path.join(main_webm.output_dir, 'report.txt'), 'a', newline='') as f:
            f.write(f"MySQL table : {db_tb}\n")
            f.write(f"MySQL '{db_tb}' table Columns : {tuple(df1.columns)}\n")
            f.write(f"MySQL '{db_tb}' table Columns count : {df1.shape[1]}\n")
            f.write(f"MySQL '{db_tb}' table records count : {len(df1.index)}\n\n")

        print(f"MySQL table : {db_tb}\n")
        print(f"MySQL '{db_tb}' table Columns : {tuple(df1.columns)}\n")
        print(f"MySQL '{db_tb}' table Columns count : {df1.shape[1]}\n")
        print(f"MySQL '{db_tb}' table records count : {len(df1.index)}\n\n")

        return df1  # print(data)
    except Exception as e:
        print(e)
        with open(os.path.join(main_webm.output_dir, 'errors.txt'), 'a', newline='') as f:
            f.write(f"\n\n--- Exception TD {datetime.now().replace(microsecond=0)}---\n{e}")
            main_webm.status = "ERROR: [MySQL DF] Something Went Wrong !!"


def details(db_tb):
    try:
        connect = create_engine("mysql://root:root@localhost/world")
        df1 = pd.read_sql(f'select * from {db_tb} limit 1;', connect)

        with connect.begin() as cursor:
            rec = cursor.execute(text(f'select count(*) from {db_tb};'))
            records = rec.fetchall()
        # print(records[0][0])
        with open(os.path.join(main_webm.output_dir, 'report.txt'), 'a', newline='') as f:
            f.write(f"MySQL table : {db_tb}\n")
            f.write(f"MySQL '{db_tb}' table Columns : {tuple(df1.columns)}\n")
            f.write(f"MySQL '{db_tb}' table Columns count : {df1.shape[1]}\n")
            f.write(f"MySQL '{db_tb}' table records count : {records[0][0]}\n\n")
    except Exception as e:
        print(e)
        with open(os.path.join(main_webm.output_dir, 'errors.txt'), 'a', newline='') as f:
            f.write(f"\n\n--- Exception TD details {datetime.now().replace(microsecond=0)}---\n{e}")
            main_webm.status = "[MySQL Details] - Something Went Wrong !!"

# print(details("country"))
