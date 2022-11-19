import ftplib
import io
import os
from datetime import datetime

import pandas as pd

from autocompy import config
from autocompy import main_webm


def cols(ftp_file_path, specific_cols):
    path = f"ftp://{config.ftp_username}:{config.ftp_password}@{config.ftp_host}:{config.ftp_port}/{ftp_file_path}"
    try:
        if specific_cols[0] in ["*", ""]:

            df1 = pd.read_csv(path, sep="\s+|;|:|\t|,", engine='python', nrows=1)

        else:

            df1 = pd.read_csv(path, sep="\s+|;|:|\t|,", engine='python', usecols=specific_cols, nrows=1)

        return list(df1.columns)
    except Exception as e:
        print(e)
        with open(os.path.join(main_webm.output_dir, 'errors.txt'), 'a', newline='') as f:
            f.write(f"\n\n--- Exception FTP {datetime.now().replace(microsecond=0)}---\n{e}")
            main_webm.status = "ERROR: [FTP Columns] - Something Went Wrong !!"


# Create Dataframe from FTP object, Count the number of rows, columns and get columns names
def dataframe(ftp_file_path, specific_cols):
    # print("\n --------------------------- FTP SOURCE -----------------------------\n")
    print("FTP file path: ", ftp_file_path + "\n")
    # print("\n")
    try:
        path = f"ftp://{config.ftp_username}:{config.ftp_password}@{config.ftp_host}:{config.ftp_port}/{ftp_file_path}"

        if specific_cols[0] in ["*", ""]:
            details(ftp_file_path)

            return pd.read_csv(path, sep="\s+|;|:|\t|,", engine='python')

        else:

            df1 = pd.read_csv(path, sep="\s+|;|:|\t|,", engine='python', usecols=specific_cols)

            with open(os.path.join(main_webm.output_dir, 'report.txt'), 'a', newline='') as f:
                f.write(f"Ftp file Path : {ftp_file_path}\n")
                f.write(f"Ftp file Columns : {tuple(df1.columns)}\n")
                f.write(f"Ftp file Columns count : {df1.shape[1]}\n")
                f.write(f"Ftp file records count : {len(df1.index)}\n\n")

            print("Ftp file Columns :", tuple(df1.columns))
            print("Ftp file Columns count :", df1.shape[1])
            print("Ftp file records count :", len(df1.index), "\n")
            return df1

    except Exception as e:
        print(e)
        with open(os.path.join(main_webm.output_dir, 'errors.txt'), 'a', newline='') as f:
            f.write(f"\n\n--- Exception FTP {datetime.now().replace(microsecond=0)}---\n{e}")
            main_webm.status = "ERROR: [FTP DataFrame] - Something Went Wrong !!"


def details(ftp_file_path):
    try:
        path = f"ftp://{config.ftp_username}:{config.ftp_password}@{config.ftp_host}:{config.ftp_port}/{ftp_file_path}"
        # get no of records

        df1 = pd.read_csv(path, sep="\s+|;|:|\t|,", engine='python', usecols=[0])

        # get columns
        ftp = ftplib.FTP()
        ftp.connect(config.ftp_host, int(config.ftp_port))
        ftp.login(config.ftp_username, config.ftp_password)

        file = io.BytesIO()

        conn = ftp.transfercmd(f"RETR {ftp_file_path}")
        b_file = conn.makefile('rb')

        for _ in range(1):
            line = b_file.readline(ftp.maxline + 1)
            file.write(line)

        b_file.close()
        conn.close()

        cols = (str(file.getvalue())[2:-5]).replace('"', '').split(',')

        col_count = len(cols)

        with open(os.path.join(main_webm.output_dir, 'report.txt'), 'a', newline='') as f:
            f.write(f"Ftp file Path : {ftp_file_path}\n")
            f.write(f"Ftp file Columns : {cols}\n")
            f.write(f"Ftp file Columns count : {col_count}\n")
            f.write(f"Ftp file records count : {len(df1.index)}\n\n")

        print("Ftp file Columns :", cols)
        print("Ftp file Columns count :", col_count)
        print("Ftp file records count :", len(df1.index), "\n")
        return cols

    except Exception as e:
        print(e)
        with open(os.path.join(main_webm.output_dir, 'errors.txt'), 'a', newline='') as f:
            f.write(f"\n\n--- Exception FTP details {datetime.now().replace(microsecond=0)}---\n{e}")
            main_webm.status = "[FTP Details] - Something Went Wrong !!"
    # logging.error("Exception: %s",e)

# df=dataframe("/idea_demo/synthetic_data_200_000.csv",["*"])
# print(df.info())
#    /ideadatamigration/fullload/IDEA_TEST_PHASE_STG.ABC_T_STG_FIN_LOAN.txt
# details("/facts/facts/INVENTORIES_INDEX.csv")
