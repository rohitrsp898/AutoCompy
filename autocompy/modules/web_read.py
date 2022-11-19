from autocompy import config
from autocompy import main_webm
import pandas as pd
from datetime import datetime
import os


def cols(web_url_path, specific_cols):
    path = web_url_path
    try:
        if specific_cols[0] in ["*", ""]:

            df1 = pd.read_csv(path, sep="\s+|;|:|\t|,", engine='python', nrows=1)

        else:

            df1 = pd.read_csv(path, sep="\s+|;|:|\t|,", engine='python', usecols=specific_cols, nrows=1)

        return list(df1.columns)
    except Exception as e:
        print(e)
        with open(os.path.join(main_webm.output_dir, 'errors.txt'), 'a', newline='') as f:
            f.write(f"\n\n--- Exception WEB_DATA {datetime.now().replace(microsecond=0)}---\n{e}")
            main_webm.status = "ERROR: [WEB DATA Columns] - Something Went Wrong !!"


# Create Dataframe from FTP object, Count the number of rows, columns and get columns names
def dataframe(web_url_path, specific_cols):
    # print("\n --------------------------- FTP SOURCE -----------------------------\n")
    print("WEB URL path: ", web_url_path + "\n")
    # print("\n")
    try:
        path = web_url_path

        if specific_cols[0] in ["*", ""]:
            details(web_url_path)

            return pd.read_csv(path, sep="\s+|;|:|\t|,", engine='python')

        else:

            df1 = pd.read_csv(path, sep="\s+|;|:|\t|,", engine='python', usecols=specific_cols)

            with open(os.path.join(main_webm.output_dir, 'report.txt'), 'a', newline='') as f:
                f.write(f"WEB URL Path : {web_url_path}\n")
                f.write(f"WEB URL Columns : {tuple(df1.columns)}\n")
                f.write(f"WEB URL Columns count : {df1.shape[1]}\n")
                f.write(f"WEB URL records count : {len(df1.index)}\n\n")

            print("WEB URL Columns :", tuple(df1.columns))
            print("WEB URL Columns count :", df1.shape[1])
            print("WEB URL records count :", len(df1.index), "\n")
            return df1

    except Exception as e:
        print(e)
        with open(os.path.join(main_webm.output_dir, 'errors.txt'), 'a', newline='') as f:
            f.write(f"\n\n--- Exception WEB URL {datetime.now().replace(microsecond=0)}---\n{e}")
            main_webm.status = "ERROR: [WEB URL DataFrame] - Something Went Wrong !!"


def details(web_url_path):
    try:
        path = web_url_path
        # get no of records

        df1 = pd.read_csv(path, sep="\s+|;|:|\t|,", engine='python', usecols=[0])
        df_cols = pd.read_csv(path, sep="\s+|;|:|\t|,", engine='python', nrows=1)


        with open(os.path.join(main_webm.output_dir, 'report.txt'), 'a', newline='') as f:
            f.write(f"WEB URL Path : {web_url_path}\n")
            f.write(f"WEB URL Columns : {tuple(df_cols.columns)}\n")
            f.write(f"WEB URL Columns count : {len(df_cols.columns)}\n")
            f.write(f"WEB URL records count : {len(df1.index)}\n\n")

        print("WEB URL Columns :", cols)
        print("WEB URL Columns count :", len(df_cols.columns))
        print("WEB URL records count :", len(df1.index), "\n")
        return cols

    except Exception as e:
        print(e)
        with open(os.path.join(main_webm.output_dir, 'errors.txt'), 'a', newline='') as f:
            f.write(f"\n\n--- Exception WEB URL details {datetime.now().replace(microsecond=0)}---\n{e}")
            main_webm.status = "[WEB URL Details] - Something Went Wrong !!"
    # logging.error("Exception: %s",e)


