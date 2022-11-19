from datetime import datetime

import os
import pandas as pd
import teradatasql

from autocompy import config
from autocompy import main_webm


def cols(db_sch_tb, specific_cols):
    try:

        with teradatasql.connect(host=config.td_host, user=config.td_username,
                                 password=config.td_password) as connection:

            if specific_cols[0] in ["*", ""]:
                df1 = pd.read_sql(f'''SELECT * from {db_sch_tb} limit 1''', connection)
            else:
                df1 = pd.read_sql(f'''SELECT {",".join(specific_cols)} from {db_sch_tb} limit 1''', connection)

        return list(df1.columns)


    except Exception as e:
        print("Exception", e)
        with open(os.path.join(main_webm.output_dir, 'errors.txt'), 'a', newline='') as f:
            f.write(f"\n\n--- Exception {datetime.now().replace(microsecond=0)}---\n{e}")
            main_webm.status = "[Teradata Cols] - Something Went Wrong !!"


def dataframe(db_tb, specific_cols):
    try:
        with teradatasql.connect(host=config.td_host, user=config.td_username, password=config.td_password) as connect:

            if specific_cols[0] in ["*", ""]:
                df1 = pd.read_sql(f'select * from {db_tb}', connect)
            else:
                df1 = pd.read_sql(f'''select {",".join(specific_cols)} from {db_tb}''', connect)

            with open(os.path.join(main_webm.output_dir, 'report.txt'), 'a', newline='') as f:
                f.write(f"Teradata table : {db_tb}\n")
                f.write(f"Teradata '{db_tb}' table Columns : {tuple(df1.columns)}\n")
                f.write(f"Teradata '{db_tb}' table Columns count : {df1.shape[1]}\n")
                f.write(f"Teradata '{db_tb}' table records count : {len(df1.index)}\n\n")

            print(f"Teradata table : {db_tb}\n")
            print(f"Teradata '{db_tb}' table Columns : {tuple(df1.columns)}\n")
            print(f"Teradata '{db_tb}' table Columns count : {df1.shape[1]}\n")
            print(f"Teradata '{db_tb}' table records count : {len(df1.index)}\n\n")

        return df1  # print(data)
    except Exception as e:
        print(e)
        with open(os.path.join(main_webm.output_dir, 'errors.txt'), 'a', newline='') as f:
            f.write(f"\n\n--- Exception TD {datetime.now().replace(microsecond=0)}---\n{e}")
            main_webm.status = "ERROR: [Teradata DF] Something Went Wrong !!"


def details(db_tb):
    try:
        with teradatasql.connect(host=config.td_host, user=config.td_username, password=config.td_password) as connect:
            df1 = pd.read_sql(f'select top 1 * from {db_tb};', connect)
            cursor = connect.cursor()
            rec = cursor.execute(f'select count(*) from {db_tb};')
            records = rec.fetchall()
            # print(records[0][0])
            with open(os.path.join(main_webm.output_dir, 'report.txt'), 'a', newline='') as f:
                f.write(f"Teradata table : {db_tb}\n")
                f.write(f"Teradata '{db_tb}' table Columns : {tuple(df1.columns)}\n")
                f.write(f"Teradata '{db_tb}' table Columns count : {df1.shape[1]}\n")
                f.write(f"Teradata '{db_tb}' table records count : {records[0][0]}\n\n")
    except Exception as e:
        print(e)
        with open(os.path.join(main_webm.output_dir, 'errors.txt'), 'a', newline='') as f:
            f.write(f"\n\n--- Exception TD details {datetime.now().replace(microsecond=0)}---\n{e}")
            main_webm.status = "[Teradata Details] - Something Went Wrong !!"

# print(details("IDEA_QA_TEST_DB.IDEA_Perf_Load_Test15"))
