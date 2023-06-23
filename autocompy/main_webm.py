import multiprocessing
import os
from datetime import datetime
from multiprocessing.pool import ThreadPool
from sqlalchemy import insert, create_engine
from autocompy import config
from autocompy.autocompy_audit import autocompy_stats

import datacompy
import numpy as np
import pandas as pd
from pandas import DataFrame
from flask_login import current_user

from autocompy import log
from autocompy.modules import local, ftp, mysql_, oracle, sftp, teradata, s3_sink, s3_source, snow, snowqa, web_read

# set pandas display columns limit to 101 columns
pd.set_option('display.max_rows', 101)
# start = datetime.now().replace(microsecond=0)  # Start time
# print("starts at --", start, "\n")

# start worker processes based on system process cpu cores
pool = ThreadPool(processes=multiprocessing.cpu_count())


# global variables
status = ''
job_status = 'Error'
output_dir = ''
total_time = 0


def time_matrics(func):
    def wrapper(*args, **kwargs):
        global total_time
        start_time = datetime.now().replace(microsecond=0)
        result = func(*args, **kwargs)
        end_time = datetime.now().replace(microsecond=0)
        total_time = end_time - start_time

        autocompy_stats(result, total_time, start_time)
        return result

    return wrapper


def get_dfs(source: str, source_path: str, sink: str, sink_path: str, specific_cols: list) -> tuple:
    """
    read source and sink and generate dataframes
    :param source: source type
    :param source_path: source path
    :param sink: sink type
    :param sink_path: sink path
    :param specific_cols: specific columns should be present in both dataframes
    :return: dataframes
    """

    if source in ['s3_source', 'teradata', 'ftp', 'sftp', 'oracle', 'mysql_', 'local'] and \
            sink in ['s3_sink', 'snow', 'local', 'mysql_', 'web_read']:

        log.info("Inside get_dfs ")
        log.info("async_r1 and r2 started")
        async_result1 = pool.apply_async(eval(source).dataframe, (source_path, specific_cols))
        async_result2 = pool.apply_async(eval(sink).dataframe, (sink_path, specific_cols))
        log.info("async_r1 and r2 completed")
        df_source = async_result1.get()
        df_sink = async_result2.get()
        log.info("Dataframes created")
        return df_source, df_sink
    else:
        print('source error ! please check names and gets_df function')
        log.info("get_dfs: Dataframes creating error !")


def out_file(mode, source=None, sink=None):
    """
    Creates empty reports and errors text files
    :return: output directories
    """
    global output_dir
    output_dir = os.path.join(os.getcwd(), "output",
                              f"{mode}_{source}_{sink}_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}")
    os.makedirs(output_dir)
    with open(os.path.join(output_dir, 'report.txt'), 'w') as f:
        f.write("--------- Summary Report ----------\n")
    with open(os.path.join(output_dir, 'errors.txt'), 'w') as f:
        f.write("-------- Error Logs --------\n")

    return output_dir


@time_matrics
def main(source: str, sink: str, source_path: str, sink_path: str, specific_cols: list):
    """
    main function do all the processing and comparison of dataframe like Nulls check, datatype check and
    generates reports.
    :param source: source type
    :param sink: sink type
    :param source_path: source path
    :param sink_path: sink path
    :param specific_cols: specific columns should be present in both dataframes
    :return: generate reports
    """

    try:
        log.info("Inside Main_webm : Main")
        global status
        global start
        global job_status

        out_file("complete", source, sink)

        log.info("MAIN : pool Src and sink started")
        async_src = pool.apply_async(eval(source).cols, (source_path, specific_cols))
        async_sink = pool.apply_async(eval(sink).cols, (sink_path, specific_cols))

        src_cols = async_src.get()
        sink_cols = async_sink.get()
        log.info("MAIN : pool Src and sink Completed")

        if specific_cols[0] in ['*', '', ' ']:
            check = all(cols in src_cols for cols in sink_cols)
        else:
            check = all(cols in src_cols for cols in specific_cols) and all(cols in sink_cols for cols in specific_cols)

        print(src_cols, sink_cols)
        # print(check)
        log.info(f"MAIN : Src and sink columns {src_cols}, {sink_cols}")

        # column match code
        if not check:
            print("\n Validation Failed ! Columns are NOT equal !\n")
            with open(os.path.join(output_dir, 'report.txt'), 'a') as f:
                f.write(f"\nColumns are NOT equal !\n")
                f.write(f"All Source Columns : {src_cols}\n")
                f.write(f"All Sink Columns : {sink_cols}\n")
                f.write(f"Specified Columns : {specific_cols}\n")
            status = "Validation Failed ! Columns are NOT equal !"
            log.info("MAIN : pool Src and sink columns not equal")

        else:

            log.info("MAIN : get_dfs called")

            # generate source and sink dataframes
            df_source, df_sink = get_dfs(source, source_path, sink, sink_path, specific_cols)

            pool.apply_async(df_dtypes, (df_source, df_sink,))  # data types comparison
            pool.apply_async(df_null, (df_source, df_sink,))  # null values comparison
            log.info("MAIN : multiprocessing started for src and sink df")

            log.info("MAIN : reports called")
            reports(df_source, df_sink)  # dataframe comparison

            log.info("MAIN : compare called")
            compare(df_source, df_sink)

            # # It will compare and store data into CSV file
            log.info("MAIN : process called")
            process(df_source, df_sink)

            print("execution complete")
            log.info("MAIN : execution complete")
            job_status = "Success"

    except Exception as e:
        print(e)
        with open(os.path.join(output_dir, 'errors.txt'), 'a', newline='') as f:
            f.write(f"\n\n--- Exception Main {datetime.now().replace(microsecond=0)}---\n{e}")
        status = "ERROR: [Main] - Something Went Wrong !! Please check error info !"
        job_status = "Failed"

    finally:

        output_dict = {"source": source, "source_path": source_path, "target": sink, "target_path": sink_path,
                       "job_status": job_status, "status": status, "mode": "Complete"}
        return output_dict


def reports(df_source: DataFrame, df_sink: DataFrame):
    """
    Generates detailed report about comparison data.
    :param df_source: source dataframe
    :param df_sink: sink dataframe
    :return: none
    """

    try:
        compare_details = datacompy.Compare(
            df_source,
            df_sink,
            join_columns=list(df_source.columns),  # You can also specify a list of columns
            abs_tol=0,  # Optional, defaults to 0
            rel_tol=0,  # Optional, defaults to 0
            df1_name='SOURCE',  # Optional, defaults to 'df1'
            df2_name='SINK'  # Optional, defaults to 'df2'
        )
        with open(os.path.join(output_dir, 'report.txt'), 'a') as f:
            f.write(compare_details.report())

    except Exception as e:
        print(e)
        with open(os.path.join(output_dir, 'errors.txt'), 'a', newline='') as f:  # Write unmatched records to csv file
            f.write(f"\n--- Exception [reports] {datetime.now().replace(microsecond=0)} ---\n{e}\n")


def compare(source_df: DataFrame, sink_df: DataFrame):
    """
    Compare the source and sink dataframes and store result in csv file.
    :param source_df: source dataframe
    :param sink_df: sink dataframe
    :return: none
    """
    try:
        comp = source_df.compare(sink_df, align_axis=0).rename(index={'self': 'Source', 'other': 'Sink'},
                                                               level=-1)  # Compare the dataframes
        if comp.shape[0] > 0:  # If there are any differences in dataframes
            with open(os.path.join(output_dir, 'Compared_records.csv'), 'w',
                      newline='') as f:  # Write the differences to csv file
                comp.to_csv(f)
                f.write("\n")
    except Exception as e:
        print(e)
        with open(os.path.join(output_dir, 'errors.txt'), 'a', newline='') as f:  # Write unmatched records to csv file
            f.write(f"\n\n--- Exception [compare] {datetime.now().replace(microsecond=0)}---\n{e}\n")


def df_dtypes(source_df: DataFrame, sink_df: DataFrame):
    """
    checks datatype of source and sink and add details to reports
    :param source_df: source dataframe
    :param sink_df: sink dataframe
    :return: none
    """
    try:
        print("---Source Dtypes : ", source_df.dtypes)
        print("----Sink Dtypes : ", sink_df.dtypes)

        with open(os.path.join(output_dir, 'report.txt'), 'a') as f:
            f.write(f"\nData Type Comparison\n")
            f.write(f"-----------------------\n")

            f.write(f"SOURCE Dtypes : \n\n{source_df.dtypes}\n\n\n")
            f.write(f"SINK Dtypes : \n\n{sink_df.dtypes}\n\n")

    except Exception as e:
        print(e)
        with open(os.path.join(output_dir, 'errors.txt'), 'a', newline='') as f:  # Write unmatched records to csv file
            f.write(f"\n\n--- Exception [df_dtypes] {datetime.now().replace(microsecond=0)}---\n{e}\n")


def df_null(source_df: DataFrame, sink_df: DataFrame):
    """
    check nulls in source and sink dataframes and add details to reports
    :param source_df: source dataframe
    :param sink_df: sink dataframe
    :return: none
    """
    try:
        if source_df.isnull().sum().sum() == 0 and sink_df.isnull().sum().sum() == 0:
            print("\nSource and Sink have NO NULL values/records !\n")
            null = "SOURCE and SINK have NO NULL values/records !"
        else:
            print("\nSource and Sink have NULL values/records !\n")
            null = "SOURCE and SINK have NULL values/records !"

        tot_src_nulls = source_df.isnull().sum().sum()
        tot_sink_nulls = sink_df.isnull().sum().sum()

        col_src_nulls = source_df.isnull().sum()
        col_sink_nulls = sink_df.isnull().sum()

        print("\n Total Nulls in Source : ", tot_src_nulls)
        print("Total Nulls in Sink : ", tot_sink_nulls)

        print("\nColumn wise Nulls in Source : \n", col_src_nulls)
        print("Column wise Nulls in Sink : \n", col_sink_nulls)

        with open(os.path.join(output_dir, 'report.txt'), 'a') as f:
            f.write(f"\nNull Comparison \n")
            f.write(f"-------------------\n")
            f.write(f"\n{null}\n\n")
            f.write(f"\nTotal NULLs in SOURCE : {tot_src_nulls}")
            f.write(f"\nTotal NULLs in SINK : {tot_sink_nulls}\n\n")
            f.write(f"\nColumn wise NULLs in SOURCE \n\n{col_src_nulls}\n")
            f.write(f"\nColumn wise NULLs in SINK \n\n{col_sink_nulls}\n\n")

    except Exception as e:
        print(e)
        with open(os.path.join(output_dir, 'errors.txt'), 'a', newline='') as f:  # Write unmatched records to csv file
            f.write(f"\n\n--- Exception [NULL Comparison] {datetime.now().replace(microsecond=0)}---\n{e}\n")


def file_generate(un_df: DataFrame):
    """
    generates different comparison csv files
    :param un_df: comparison dataframe
    :return: none
    """
    global status

    if un_df[lambda x: x['_merge'] == 'both'].shape[0] > 0:
        with open(os.path.join(output_dir, 'Matched_records.csv'), 'w',
                  newline='') as f:  # Write unmatched records to csv file
            un_df[lambda x: x['_merge'] == 'both'].head(50).drop(['count'], axis=1).to_csv(f, index_label='INDEX')
            f.write("\n")

    if un_df[(un_df["_merge"] == 'Sink') & (un_df["count"] != 0)].shape[0] > 0:
        with open(os.path.join(output_dir, 'Sink_Duplicate_records.csv'), 'w',
                  newline='') as f:  # Write unmatched records to csv file
            un_df[(un_df["_merge"] == 'Sink') & (un_df["count"] != 0)].head(50).groupby(
                un_df.columns.tolist()[:-4]).size().reset_index().rename(columns={0: 'No of extra records'}).to_csv(f,
                                                                                                                    index_label='INDEX')
            f.write("\n")

    if un_df[(un_df["_merge"] == 'Source') & (un_df["count"] != 0)].shape[0] > 0:
        with open(os.path.join(output_dir, 'Source_Duplicate_records.csv'), 'w',
                  newline='') as f:  # Write unmatched records to csv file
            un_df[(un_df["_merge"] == 'Source') & (un_df["count"] != 0)].head(50).groupby(
                un_df.columns.tolist()[:-4]).size().reset_index().rename(columns={0: 'No of extra records'}).to_csv(f,
                                                                                                                    index_label='INDEX')
            f.write("\n")

    if 0 < un_df[(un_df["_merge"] == 'Source') & (un_df["count"] == 0)].shape[0] < 50:
        with open(os.path.join(output_dir, 'Missing_records.csv'), 'w',
                  newline='') as f:  # 0 present is source only 1 present in source and sink
            un_df[(un_df["_merge"] == 'Source') & (un_df["count"] == 0)].head(50).drop(['count'], axis=1).to_csv(f,
                                                                                                                 index_label='INDEX')
            f.write("\n")

    elif un_df[(un_df["_merge"] == 'Source') & (un_df["count"] == 0)].shape[0] != 0:
        with open(os.path.join(output_dir, 'Missing_records.csv'), 'w', newline='') as f:
            un_df[(un_df["_merge"] == 'Source') & (un_df["count"] == 0)].head(50) \
                .drop(['count'], axis=1).head(50).to_csv(f, index_label='INDEX')
            f.write("\n")
        print("\nMore than 50 records are Missing !!.. Validation Failed !\n")
        with open(os.path.join(output_dir, 'report.txt'), 'a', newline='') as f:
            f.write("\nMore than 50 records are Missing !!.. Validation Failed !\n")
        status = "STATUS: More than 50 records are Missing !!.. Validation Failed !"

        with open(os.path.join(output_dir, 'errors.txt'), 'a', newline='') as f:  # Write unmatched records to csv file
            f.write(f"\n--- Message {datetime.now().replace(microsecond=0)}\
                            ---\nMore than 50 records are Missing !!.. Validation Failed !\n")
        print("\nDataframe have shuffled/mismatched records")
        status = "STATUS: More than 50 records are MisMatched !!.. Validation Failed !"

    if 0 < un_df[lambda x: x['_merge'] != 'both'].shape[0] < 50:
        with open(os.path.join(output_dir, 'Mismatched_records.csv'), 'w',
                  newline='') as f:  # Write unmatched records to csv file
            un_df[lambda x: x['_merge'] != 'both'].drop(['count'], axis=1).to_csv(f, index_label='INDEX')
            f.write("\n")

        with open(os.path.join(output_dir, 'errors.txt'), 'a', newline='') as f:  # Write unmatched records to csv file
            f.write(f"\n--- Message {datetime.now().replace(microsecond=0)} \
                            ---\nDataframe have shuffled/mismatched records\n")
        print("\nDataframe have shuffled/mismatched records")
        status = "STATUS: Dataframe have shuffled/mismatched records"

    elif un_df[lambda x: x['_merge'] != 'both'].shape[0] != 0:
        with open(os.path.join(output_dir, 'Mismatched_records.csv'), 'w', newline='') as f:
            un_df[lambda x: x['_merge'] != 'both'].drop(['count'], axis=1).head(50).to_csv(f, index_label='INDEX')
            f.write("\n")
        print("\nMore than 50 records are Missing !!.. Validation Failed !\n")
        with open(os.path.join(output_dir, 'report.txt'), 'a', newline='') as f:
            f.write("\nMore than 50 records are MisMatched !!.. Validation Failed !\n")

        with open(os.path.join(output_dir, 'errors.txt'), 'a', newline='') as f:  # Write unmatched records to csv file
            f.write(f"\n--- Message {datetime.now().replace(microsecond=0)} \
                            ---\nMore than 50 records are Mismatched !!.. Validation Failed !\n")
        print("\nDataframe have shuffled/mismatched records")
        status = "STATUS: More than 50 records are Mismatched !!.. Validation Failed !"

    else:
        print("\nDataframe is Equal with different Data Types.\n")
        with open(os.path.join(output_dir, 'report.txt'), 'a', newline='') as f:
            f.write("\nDataframe is Equal with different Data Types.\n")
        status = "STATUS: Dataframe is Equal with different Data Types."


# Get unmatched records from Source and Sink & compare both dataframes
def process(source_df: DataFrame, sink_df: DataFrame):
    """
    using source and sink dataframe compare the dataframes and return compared dataframe to file_generate function
    :param source_df: source dataframe
    :param sink_df: sink dataframe
    :return: none
    """

    global status
    try:
        if sink_df.equals(source_df):  # Check if both dataframes are equal
            print("\nDataframe is Equal")
            # global status
            status = "STATUS: Dataframe is Equal !"

        elif source_df.columns.equals(sink_df.columns):

            print("---- Source Dataframe ----\n", source_df)
            print("---- Sink Dataframe ----\n", sink_df)

            source_df['count'] = source_df.groupby(list(source_df.columns)).cumcount()
            sink_df['count'] = sink_df.groupby(list(sink_df.columns)).cumcount()

            source_df['index'] = source_df.index
            source_df = source_df.reset_index(drop=True)
            sink_df['index'] = sink_df.index
            sink_df = sink_df.reset_index(drop=True)

            # Get the unmatched records from Source and Sink using outer join method
            # Note: It will compare based on count with all the filed on dataframes to avoid duplicates
            un_df = source_df.merge(sink_df, how='outer', on=list(source_df.columns[:-1]), indicator=True)
            # print(un_df)

            # Rename the column value of _merge to Source or Sink instead of left_only or right_only
            un_df["_merge"].replace({"left_only": "Source", "right_only": "Sink"}, inplace=True)

            # Rename the column index_x to Source_index and index_y to Sink_index
            un_df.rename(columns={'index_x': 'Source_index', 'index_y': "Sink_index"}, inplace=True)
            un_df["Source_index"].replace({np.nan: "Missing"}, inplace=True)
            un_df["Sink_index"].replace({np.nan: "Missing"}, inplace=True)

            print('--------------------------', un_df)

            file_generate(un_df)

        else:
            with open(os.path.join(output_dir, 'errors.txt'), 'a',
                      newline='') as f:  # Write unmatched records to csv file
                f.write(f"\n--- Message  {datetime.now().replace(microsecond=0)} \
                                    ---\nSource and Sink Dataframes have different columns.")
            print("\nDataframes have different columns.....")
            status = "STATUS: Source and Sink Dataframes have different columns. "

    except Exception as e:
        print(e)
        with open(os.path.join(output_dir, 'errors.txt'), 'a', newline='') as f:  # Write unmatched records to csv file
            f.write(f"\n--- Exception [process] {datetime.now().replace(microsecond=0)} ---\n{e}")
        status = "ERROR: Something Went Wrong! Please provide correct details!!"


@time_matrics
def details(source: str, sink: str, source_path: str, sink_path: str):
    """
    add dataframe basic details like columns name, count etc
    :param source: source type
    :param sink: sink type
    :param source_path: source path
    :param sink_path: sink path
    :return: none
    """

    try:
        log.info("DETAIL : INSIDE DETAILS")
        out_file("basic", source, sink)
        global status

        log.info("DETAIL : BASIC DETAILS started")
        pool.apply_async(eval(source).details, (source_path,)).get()
        pool.apply_async(eval(sink).details, (sink_path,)).get()

        print("\nBasic Details function done !\n")
        log.info("DETAIL : Basic Details function done !")
        job_status = "Success"
        status = "Details fetched successfully !"

    except Exception as e:
        print("Detail--> ", e)
        with open(os.path.join(output_dir, 'errors.txt'), 'a', newline='') as f:
            f.write(f"\n\n--- Exception DETAIL {datetime.now().replace(microsecond=0)}---\n{e}")
        status = f"ERROR: DETAILS - {e}"
        job_status = "Failed"

    finally:
        output_dict = {"source": source, "source_path": source_path, "target": sink, "target_path": sink_path,
                       "job_status": job_status, "status": status, "mode": "Basic"}
        return output_dict
