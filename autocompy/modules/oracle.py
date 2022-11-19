from datetime import datetime
import cx_Oracle
from autocompy import config
from autocompy import main_webm
import pandas as pd
import logging, os


def cols(table, specific_cols):
    try:
        dsn_tns = cx_Oracle.makedsn(config.o_hostname, config.o_port, config.o_sid)
        # print(dsn_tns)
        # Create connection with oracle database
        con = cx_Oracle.connect(config.o_username, config.o_password, dsn_tns)

        if specific_cols[0] in ["*", ""]:
            df1 = pd.read_sql(f'''SELECT * FROM {table} WHERE ROWNUM = 1''', con)
        else:
            df1 = pd.read_sql(f'''select {",".join(specific_cols)} from {table} WHERE ROWNUM = 1''', con)

        return list(df1.columns)


    except Exception as e:
        print("Exception", e)
        with open(os.path.join(main_webm.output_dir, 'errors.txt'), 'a', newline='') as f:
            f.write(f"\n\n--- Exception {datetime.now().replace(microsecond=0)}---\n{e}")
            main_webm.status = "[Oracle Cols] - Something Went Wrong !!"


def dataframe(table, specific_cols):
    try:
        # making dsn string
        dsn_tns = cx_Oracle.makedsn(config.o_hostname, config.o_port, config.o_sid)
        # print(dsn_tns)
        # Create connection with oracle database
        connect = cx_Oracle.connect(config.o_username, config.o_password, dsn_tns)
        # print(connect)

        # df1=pd.read_sql(f'''SELECT * FROM {table}''',connect)
        if specific_cols[0] in ["*", ""]:
            df1 = pd.read_sql(f'select * from {table}', connect)
        else:
            df1 = pd.read_sql(f'''select {",".join(specific_cols)} from {table}''', connect)

        with open(os.path.join(main_webm.output_dir, 'report.txt'), 'a', newline='') as f:
            f.write(f"ORACLE table : {table}\n")
            f.write(f"ORACLE '{table}' table Columns : {tuple(df1.columns)}\n")
            f.write(f"ORACLE '{table}' table Columns count : {df1.shape[1]}\n")
            f.write(f"ORACLE '{table}' table records count : {len(df1.index)}\n\n")

        print("ORACLE table: ", table)
        print(f"ORACLE '{table}' table Columns :", tuple(df1.columns))
        print(f"ORACLE '{table}' table Columns count :", df1.shape[1])
        print(f"ORACLE '{table}' table records count :", len(df1.index), "\n")
        return df1

    except Exception as e:
        print(e)
        with open(os.path.join(main_webm.output_dir, 'errors.txt'), 'a', newline='') as f:
            f.write(f"\n\n--- Exception {datetime.now().replace(microsecond=0)}---\n{e}")
            main_webm.status = "ERROR: [Oracle Dataframe] - Something Went Wrong !!"


def details(table):
    try:
        dsn_tns = cx_Oracle.makedsn(config.o_hostname, config.o_port, config.o_sid)
        # print(dsn_tns)
        # Create connection with oracle database
        con = cx_Oracle.connect(config.o_username, config.o_password, dsn_tns)

        df1 = pd.read_sql(f'''SELECT * FROM {table} WHERE ROWNUM = 1''', con)
        cursor = con.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        records = cursor.fetchall()
        with open(os.path.join(main_webm.output_dir, 'report.txt'), 'a', newline='') as f:
            f.write(f"ORACLE table : {table}\n")
            f.write(f"ORACLE '{table}' table Columns : {tuple(df1.columns)}\n")
            f.write(f"ORACLE '{table}' table Columns count : {df1.shape[1]}\n")
            f.write(f"ORACLE '{table}' table records count : {records[0][0]}\n\n")

        print("ORACLE table: ", table)
        print(f"ORACLE '{table}' table Columns :", tuple(df1.columns))
        print(f"ORACLE '{table}' table Columns count :", df1.shape[1])
        print(f"ORACLE '{table}' table records count :", records[0][0], "\n")


    except Exception as e:
        print("Exception", e)
        with open(os.path.join(main_webm.output_dir, 'errors.txt'), 'a', newline='') as f:
            f.write(f"\n\n--- Exception {datetime.now().replace(microsecond=0)}---\n{e}")
            main_webm.status = "[Oracle Details] - Something Went Wrong !!"

# # Execute a statement that will generate a result set.
# sql = f"SELECT * from {db}.{schm}.{tb}"
# cursor.execute(sql)
# df=dataframe('INVENTORIES')
# df=details('INVENTORIES')
# print(df)
