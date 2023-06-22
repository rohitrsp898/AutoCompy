import json
import os
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text

from autocompy import config
from autocompy import main_webm


def get_audit_data(specific_cols):
    try:
        connect = create_engine(f"mysql://{config.m_username}:{config.m_password}@{config.m_host}/{config.m_database}")

        if specific_cols[0] in ["*", ""]:
            df1 = pd.read_sql(f'''SELECT * from autocompy.autocompy_audit''', connect)
        else:
            df1 = pd.read_sql(f'''SELECT {",".join(specific_cols)} from autocompy.autocompy_audit''', connect)

        return df1

    except Exception as e:
        print("Exception at modules under history_to_chart ", e)


def get_user_no_exc_bar_chart():
    """
    Get user with total no of execution
    :return:
    """

    df = get_audit_data(["username"])
    data = df["username"].value_counts()
    return data.to_json(orient='index')


def get_state_code_radar_chart():
    """
    Get status_code with total no of execution
    :return:
    """

    df = get_audit_data(["status_code"])

    data = df["status_code"].value_counts()
    # print("radar func", data.to_json(orient='index'))
    return data.to_json(orient='index')


def get_state_code_bar_chart():
    """
    Get status_code with total no of execution
    :return:
    """

    data_dict = get_audit_data(["username", "status_code"]).groupby('status_code')['username'].value_counts().unstack(
        fill_value=0).to_dict()

    user_name = []
    failed = []
    success = []
    error = []

    for data, values in data_dict.items():
        user_name.append(data)
        error.append(values['Error'])
        failed.append(values['Failed'])
        success.append(values['Success'])

    user_name = json.dumps(user_name)
    error = json.dumps(error)
    failed = json.dumps(failed)
    success = json.dumps(success)

    return user_name, error, failed, success


def get_total_count_based_on_date():
    data = get_audit_data(["executed_at"])
    data["execution_date"] = data['executed_at'].str.split(' ').str[0]
    count_data = data["execution_date"].value_counts()

    return count_data.to_json(orient='index')


