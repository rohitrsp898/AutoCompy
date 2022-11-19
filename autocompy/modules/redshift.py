from datetime import datetime

import pandas as pd
import redshift_connector

from autocompy import config
from autocompy import main_webm
import os


def dataframe(db_sch_tb):
    try:
        conn = redshift_connector.connect(
            host=config.red_host,
            database=config.red_db,
            user=config.red_username,
            password=config.red_password
        )

        # conn1=psycopg2.connect(
        #     host=config.red_host,
        #     database=config.red_db,
        #     user=config.red_username,
        #     password=config.red_password)

        cursor = conn.cursor()
        cursor.execute(f'''select * from {db_sch_tb}''')
        print(cursor.fetchall())
        df1 = cursor.fetch_dataframe()
        print(df1)
        print(type(df1))

        df1 = pd.read_sql(f'''SELECT * from {db_sch_tb}''', conn)
        with open(os.path.join(main_webm.output_dir, 'report.txt'), 'a', newline='') as f:
            f.write(f"Resdhift table : {db_sch_tb}\n")
            f.write(f"Resdhift '{db_sch_tb}' table Columns : {tuple(df1.columns)}\n")
            f.write(f"Resdhift '{db_sch_tb}' table Columns count : {df1.shape[1]}\n")
            f.write(f"Resdhift '{db_sch_tb}' table records count : {df1.shape[0]}\n\n")

        return df1
    except Exception as e:
        print(e)
        with open('Output/errors.txt', 'a', newline='') as f:
            f.write(f"\n\n--- Exception {datetime.now().replace(microsecond=0)}---\n{e}")
            main_webm.status = "[Redshift] Something Went Wrong !!"

# print(dataframe("qa.idea_qa_test_db.a_idea_t_stg_fin_card_qa_test"))


# print(dataframe("idea_test_phase_stg.abc_t_stg_fin_account"))
