import ftplib
import io
import os
from datetime import datetime

import pandas as pd

from autocompy import config
from autocompy import main_webm


def cols(local_file_path, specific_cols):
    path = local_file_path
    try:
        if specific_cols[0] in ["*", ""]:
            df1 = pd.read_csv(path, sep="\s+|;|:|\t|,", engine='python', nrows=1)

        else:
            df1 = pd.read_csv(path, sep="\s+|;|:|\t|,", engine='python', usecols=specific_cols, nrows=1)

        return list(df1.columns)

    except Exception as e:
        print(e)
        with open(os.path.join(main_webm.output_dir, 'errors.txt'), 'a', newline='') as f:
            f.write(f"\n\n--- Exception Local FS {datetime.now().replace(microsecond=0)}---\n{e}")
            main_webm.status = "ERROR: [LOCAL FS Columns] - Something Went Wrong !!"


# Create Dataframe from FTP object, Count the number of rows, columns and get columns names
def dataframe(local_file_path, specific_cols):
    # print("\n --------------------------- FTP SOURCE -----------------------------\n")
    print("Local file path: ", local_file_path + "\n")
    # print("\n")
    try:
        path = local_file_path

        if specific_cols[0] in ["*", ""]:
            details(local_file_path)

            return pd.read_csv(path, sep="\s+|;|:|\t|,", engine='python')

        else:

            df1 = pd.read_csv(path, sep="\s+|;|:|\t|,", engine='python', usecols=specific_cols)

            with open(os.path.join(main_webm.output_dir, 'report.txt'), 'a', newline='') as f:
                f.write(f"Local file Path : {local_file_path}\n")
                f.write(f"Local file Columns : {tuple(df1.columns)}\n")
                f.write(f"Local file Columns count : {df1.shape[1]}\n")
                f.write(f"Local file records count : {len(df1.index)}\n\n")

            print("Local file Columns :", tuple(df1.columns))
            print("Local file Columns count :", df1.shape[1])
            print("Local file records count :", len(df1.index), "\n")
            return df1

    except Exception as e:
        print(e)
        with open(os.path.join(main_webm.output_dir, 'errors.txt'), 'a', newline='') as f:
            f.write(f"\n\n--- Exception Local FS {datetime.now().replace(microsecond=0)}---\n{e}")
            main_webm.status = "ERROR: [LOCAL FS DataFrame] - Something Went Wrong !!"
    # logging.error("Exception: %s",e)


def details(local_file_path):
    try:
        path = local_file_path

        # get no of records
        df1 = pd.read_csv(path, sep="\s+|;|:|\t|,", engine='python', usecols=[0])
        df2 = pd.read_csv(path, sep="\s+|;|:|\t|,", engine='python', nrows=2)

        with open(os.path.join(main_webm.output_dir, 'report.txt'), 'a', newline='') as f:
            f.write(f"Local file Path : {local_file_path}\n")
            f.write(f"Local file Columns : {tuple(df2.columns)}\n")
            f.write(f"Local file Columns count : {len(df2.columns)}\n")
            f.write(f"Local file records count : {len(df1.index)}\n\n")

        print("Local file Columns :", tuple(df2.columns))
        print("Local file Columns count :", len(df2.columns))
        print("Local file records count :", len(df1.index), "\n")
        return cols

    except Exception as e:
        print(e)
        with open(os.path.join(main_webm.output_dir, 'errors.txt'), 'a', newline='') as f:
            f.write(f"\n\n--- Exception Local FS details {datetime.now().replace(microsecond=0)}---\n{e}")
            main_webm.status = "[LOCAL FS Details] - Something Went Wrong !!"
    # logging.error("Exception: %s",e)
