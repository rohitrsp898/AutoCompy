import os
import re
from datetime import datetime
from urllib.parse import quote

import pandas as pd
from smart_open import smart_open

from autocompy import config
from autocompy import main_webm

# /home/pfsu/cdf_data/facts/gcs_source_orders.csv

special_char = re.compile('[@_!#$%^&*()<>?/\|}{~:]')  # if special char is present in password


# logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",filename='Output/logs.log', encoding='utf-8', level=logging.DEBUG)

def cols(sftp_file_path, specific_cols):
    try:
        if special_char.search(config.sftp_password) is None:
            print("Password dont have special char.")
            path = f"sftp://{config.sftp_username}:{config.sftp_password}@{config.sftp_host}:{config.sftp_port}/{sftp_file_path}"

        else:
            print("Password have special char.")
            path = f"sftp://{config.sftp_username}:%s@{config.sftp_host}:{config.sftp_port}/{sftp_file_path}" % quote(
                config.sftp_password)

        if specific_cols[0] in ["*", ""]:

            if path.endswith('.csv'):
                df1 = pd.read_csv(smart_open(path), nrows=1)
            if path.endswith('.txt'):
                df1 = pd.read_csv(smart_open(path), sep='¬', engine='python', nrows=1)

        else:
            if path.endswith('.csv'):
                df1 = pd.read_csv(smart_open(path), usecols=specific_cols, nrows=1)
            if path.endswith('.txt'):
                df1 = pd.read_csv(smart_open(path), sep='¬', engine='python', usecols=specific_cols, nrows=1)

        return list(df1.columns)
    except Exception as e:
        print(e)
        with open(os.path.join(main_webm.output_dir, 'errors.txt'), 'a', newline='') as f:
            f.write(f"\n\n--- Exception FTP {datetime.now().replace(microsecond=0)}---\n{e}")
            main_webm.status = "ERROR: [FTP Columns] - Something Went Wrong !!"


def dataframe(sftp_file_path, specific_cols):
    # print("\n --------------------------- FTP SOURCE -----------------------------\n")
    print("sFTP file path: ", sftp_file_path + "\n")
    # print("\n")
    try:

        if special_char.search(config.sftp_password) is None:
            print("Password dont have special char.")
            path = f"sftp://{config.sftp_username}:{config.sftp_password}@{config.sftp_host}:{config.sftp_port}/{sftp_file_path}"

        else:
            print("Password have special char.")
            path = f"sftp://{config.sftp_username}:%s@{config.sftp_host}:{config.sftp_port}/{sftp_file_path}" % quote(
                config.sftp_password)

        # if '#' in config.sftp_password:
        #     path=f"sftp://{config.sftp_username}:%s@{config.sftp_host}:{config.sftp_port}/{sftp_file_path}"%quote(config.sftp_password)
        # else:
        #     path=f"sftp://{config.sftp_username}:{config.sftp_password}@{config.sftp_host}:{config.sftp_port}/{sftp_file_path}"
        # print("sFTP file path: ---->",path)
        if specific_cols[0] in ["*", ""]:
            if path.endswith('.csv'):
                df1 = pd.read_csv(smart_open(path))
            if path.endswith('.txt'):
                df1 = pd.read_csv(smart_open(path), sep='¬', engine='python')

        else:
            if path.endswith('.csv'):
                df1 = pd.read_csv(smart_open(path), usecols=specific_cols)
            if path.endswith('.txt'):
                df1 = pd.read_csv(smart_open(path), sep='¬', engine='python', usecols=specific_cols)

        with open(os.path.join(main_webm.output_dir, 'report.txt'), 'a', newline='') as f:
            f.write(f"sFtp file Path : {sftp_file_path}\n")
            f.write(f"sFtp file Columns : {tuple(df1.columns)}\n")
            f.write(f"sFtp file Columns count : {df1.shape[1]}\n")
            f.write(f"sFtp file records count : {len(df1.index)}\n\n")

        print("sFtp file Columns :", tuple(df1.columns))
        print("sFtp file Columns count :", df1.shape[1])
        print("sFtp file records count :", len(df1.index), "\n")
        return df1

    except Exception as e:
        print(e)
        with open(os.path.join(main_webm.output_dir, 'errors.txt'), 'a', newline='') as f:
            f.write(f"\n\n--- Exception SFTP {datetime.now().replace(microsecond=0)}---\n{e}")
            main_webm.status = "ERROR: [SFTP DF] Something Went Wrong !!"


def details(sftp_file_path):
    try:
        if special_char.search(config.sftp_password) is None:
            print("Password dont have special char.")
            path = f"sftp://{config.sftp_username}:{config.sftp_password}@{config.sftp_host}:{config.sftp_port}/{sftp_file_path}"

        else:
            print("Password have special char.")
            path = f"sftp://{config.sftp_username}:%s@{config.sftp_host}:{config.sftp_port}/{sftp_file_path}" % quote(
                config.sftp_password)

        if path.endswith('.csv'):
            df1 = pd.read_csv(smart_open(path), usecols=[0])
        if path.endswith('.txt'):
            df1 = pd.read_csv(smart_open(path), sep='¬', engine='python', usecols=[0])

        if path.endswith('.csv'):
            df_col = pd.read_csv(smart_open(path), nrows=1)
        if path.endswith('.txt'):
            df_col = pd.read_csv(smart_open(path), sep='¬', engine='python', nrows=1)

        with open(os.path.join(main_webm.output_dir, 'report.txt'), 'a', newline='') as f:
            f.write(f"Sftp file Path : {sftp_file_path}\n")
            f.write(f"Sftp file Columns : {tuple(df_col.columns)}\n")
            f.write(f"Sftp file Columns count : {df_col.shape[1]}\n")
            f.write(f"Sftp file records count : {len(df1.index)}\n\n")

        print("Sftp file Columns :", tuple(df1.columns))
        print("Sftp file Columns count :", df1.shape[1])
        print("Sftp file records count :", len(df1.index), "\n")
        return df1

    except Exception as e:
        print(e)
        with open(os.path.join(main_webm.output_dir, 'errors.txt'), 'a', newline='') as f:
            f.write(f"\n\n--- Exception SFTP details {datetime.now().replace(microsecond=0)}---\n{e}")
            main_webm.status = "[SFTP Details] Something Went Wrong !!"

# /home/pfsu/cdf_data/facts/INVENTORIES.csv
# path="/sftpuser1/demo-data/facts/INVENTORIES_INDEX.csv"
# new --=   /home/pfsu/cdf_data/facts/INVENTORIES_INDEX.csv
# p2="/sftpuser1/demo-data/synthetic_data_200_000/synthetic_data_200_000.csv"
# print(dataframe("/home/pfsu/cdf_data/facts/INVENTORIES_INDEX.csv",[0]))
# print("details")
# details("/home/pfsu/cdf_data/facts/INVENTORIES_INDEX.csv")
