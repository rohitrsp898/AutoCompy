
from datetime import datetime

import boto3
import json
import os
import pandas as pd
from smart_open import smart_open

from autocompy import config
from autocompy import main_webm

#logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s",filename='Output/logs.log', encoding='utf-8', level=logging.DEBUG)
# Get Column names, Columns count, rows count and return Dataframe from S3 object
s3_client = boto3.client(
                service_name=config.service_name,
                region_name=config.region_name,
                aws_access_key_id=config.aws_access_key_id,
                aws_secret_access_key=config.aws_secret_access_key)


def cols(s3_uri,specific_cols):
    if specific_cols in ["*",""]:\
        specific_cols= None
    
    try:
        bucket_name=s3_uri.split('/')[2]
        s3_file_path=s3_uri.split(bucket_name)[1][1:]
        #print("--------",s3_file_path)
        #s3_str=f's3://{config.aws_access_key_id}:{config.aws_secret_access_key}@{bucket_name}/{s3_file_path}'
        print("S3 file path: ",f"s3://{bucket_name}/{s3_file_path}")

        theobjects = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=s3_file_path)
        for obj in theobjects['Contents']:
            if obj['Key'].endswith('.csv'):
                #print(obj['Key'])
                   # Get Columns name and Columns count from S3 object
                resp = s3_client.select_object_content(
                    Bucket=bucket_name,
                    Key=obj['Key'], 
                    ExpressionType='SQL',
                    Expression='''SELECT * FROM s3object limit 1''',
                    #InputSerialization={'CSV': {"FileHeaderInfo": "Use"}, 'CompressionType': 'NONE'}, # Comment this line while reading Parquet file
                    InputSerialization={'CSV': {"FileHeaderInfo": "None"}, 'CompressionType': 'NONE'},
                    OutputSerialization={'CSV': {}},
                )
                print("Connection stablished with s3...")
                for event in resp['Payload']:
                    if 'Records' in event:
                        cols = event['Records']['Payload'].decode('utf-8')
                        #print('------',cols)
                        columns=cols.strip().split(',')
                
                
                
            if obj['Key'].endswith('.json'):
        
                 # Get Columns name and Columns count from S3 object
                resp = s3_client.select_object_content(
                    Bucket=bucket_name,
                    Key=obj['Key'], 
                    ExpressionType='SQL',
                    Expression='''SELECT * FROM s3object limit 1''',
                    #InputSerialization={'CSV': {"FileHeaderInfo": "Use"}, 'CompressionType': 'NONE'}, # Comment this line while reading Parquet file
                    InputSerialization={'JSON': {"Type": "Document"}},
                    OutputSerialization={'JSON': {}},
                )
                #print("Connection stablished with s3...")
                for event in resp['Payload']:
                    if 'Records' in event:
                        cols = event['Records']['Payload'].decode('utf-8')
                        print(list(json.loads(cols).keys()))
                        columns=list(json.loads(cols).keys())

              
            if obj['Key'].endswith('.parquet'):
                #print(obj['Key'])
                  # Get Columns name and Columns count from S3 object
                resp = s3_client.select_object_content(
                    Bucket=bucket_name,
                    Key=obj['Key'], 
                    ExpressionType='SQL',
                    Expression='''SELECT * FROM s3object limit 1''',
                    #InputSerialization={'CSV': {"FileHeaderInfo": "Use"}, 'CompressionType': 'NONE'}, # Comment this line while reading Parquet file
                    InputSerialization={'Parquet': {}},
                    OutputSerialization={'JSON': {}},
                )
                #print("Connection stablished with s3...")
                for event in resp['Payload']:
                    if 'Records' in event:
                        cols = event['Records']['Payload'].decode('utf-8')
                        print(list(json.loads(cols).keys()))
                        columns=list(json.loads(cols).keys())
                        
        return columns
                        
               




        # bucket_name=s3_uri.split('/')[2]
        # s3_file_path=s3_uri.split(bucket_name)[1][1:]

        # theobjects = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=s3_file_path)
        # for obj in theobjects['Contents']:
        #     if obj['Key'].endswith('.csv'):
        #             #print(obj['Key'])
        #         s3_str=f's3://{config.aws_access_key_id}:{config.aws_secret_access_key}@{bucket_name}/{obj["Key"]}'
        #         df1=pd.read_csv(smart_open(s3_str),usecols=specific_cols,nrows=1)
                
               
        #     if obj['Key'].endswith('.parquet'):
        #         #print(obj['Key'])
        #         s3_str=f's3://{config.aws_access_key_id}:{config.aws_secret_access_key}@{bucket_name}/{obj["Key"]}'
        #         #if specific_cols[0] in ["*",""]:
        #         df=pd.read_parquet(smart_open(s3_str),engine='pyarrow',columns=specific_cols)
        #         df1=df[specific_cols]
        #     if obj['Key'].endswith('.json'):
        #         #print(obj['Key'])
        #         s3_str=f's3://{config.aws_access_key_id}:{config.aws_secret_access_key}@{bucket_name}/{obj["Key"]}'
        #         #if specific_cols[0] in ["*",""]:
        #         df=pd.read_json(smart_open(s3_str),lines=True,nrows=1)
        #         df1=df[specific_cols]
        #         print("--------JSOn------",df.columns,df1.columns)
        # return list(df1.columns)      

    except Exception as e:
        print(e)
        with open(os.path.join(main_webm.output_dir, 'errors.txt'), 'a',newline='') as f:
            f.write(f"\n\n--- Exception Sink DF {datetime.now().replace(microsecond=0)}---\n{e}")
            main_webm.status="ERROR: [S3 Source Cols] - Something Went Wrong !!"

def dataframe(s3_uri,specific_cols):  #s3://ideakafkatest/QA_UI_testing/ftp/3/sftp/sftpuser1/demo-data/facts/INVENTORIES_INDEX.csv
    try:
        
        df_list=[]
        ty=None
        bucket_name=s3_uri.split('/')[2]     #ideakafkatest
        s3_file_path=s3_uri.split(bucket_name)[1][1:]  #QA_UI_testing/ftp/3/sftp/sftpuser1/demo-data/facts/INVENTORIES_INDEX.csv
        #print("--------",s3_file_path)
        s3_str=f's3://{config.aws_access_key_id}:{config.aws_secret_access_key}@{bucket_name}/{s3_file_path}'
        print("S3 Source file path: ",f"s3://{bucket_name}/{s3_file_path}")

        theobjects = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=s3_file_path)

        if specific_cols[0] in ["*",""]:

            for obj in theobjects['Contents']:
                if obj['Key'].endswith('.csv'):
                    #print(obj['Key'])
                    s3_str=f's3://{config.aws_access_key_id}:{config.aws_secret_access_key}@{bucket_name}/{obj["Key"]}'
                    df_list.append(pd.read_csv(smart_open(s3_str),index_col=None, header=0))
                    
                    #df_list.append(pd.read_csv(smart_open(s3_str),index_col=None, header=0, usecols=specific_cols))

                if obj['Key'].endswith('.parquet'):
                    #print(obj['Key'])
                    s3_str=f's3://{config.aws_access_key_id}:{config.aws_secret_access_key}@{bucket_name}/{obj["Key"]}'
                    #if specific_cols[0] in ["*",""]:
                    df_list.append(pd.read_parquet(smart_open(s3_str),engine='pyarrow'))
                    #else:
                    #    df_list.append(pd.read_parquet(smart_open(s3_str),engine='pyarrow',columns=specific_cols))

                if obj['Key'].endswith('.json'):
                    #print(obj['Key'])
                    s3_str=f's3://{config.aws_access_key_id}:{config.aws_secret_access_key}@{bucket_name}/{obj["Key"]}'
                    #if specific_cols[0] in ["*",""]:
                    df_list.append(pd.read_json(smart_open(s3_str),lines=True))
                    #else:
                        #df_list.append(pd.read_json(smart_open(s3_str),lines=True,orient='columns',usecols=specific_cols))
        else:

            for obj in theobjects['Contents']:
                if obj['Key'].endswith('.csv'):
                    #print(obj['Key'])
                    s3_str=f's3://{config.aws_access_key_id}:{config.aws_secret_access_key}@{bucket_name}/{obj["Key"]}'
                    
                    df_list.append(pd.read_csv(smart_open(s3_str),index_col=None, header=0, usecols=specific_cols))

                if obj['Key'].endswith('.parquet'):
                    #print(obj['Key'])
                    s3_str=f's3://{config.aws_access_key_id}:{config.aws_secret_access_key}@{bucket_name}/{obj["Key"]}'
                   
                    df_list.append(pd.read_parquet(smart_open(s3_str),engine='pyarrow',columns=specific_cols))

                if obj['Key'].endswith('.json'):
                    #print(obj['Key'])
                    ty='json'
                    s3_str=f's3://{config.aws_access_key_id}:{config.aws_secret_access_key}@{bucket_name}/{obj["Key"]}'
                    
                    df_list.append(pd.read_json(smart_open(s3_str),lines=True))
                        
                
        
        return concat_df(s3_uri,df_list,ty,specific_cols)

    except Exception as e:
        print(e)
        with open(os.path.join(main_webm.output_dir, 'errors.txt'), 'a',newline='') as f:
            f.write(f"\n\n--- Exception Source DF {datetime.now().replace(microsecond=0)}---\n{e}")
            main_webm.status="ERROR: [S3 Source DF] - Something Went Wrong !!"
        #logging.error("Exception: %s",e).

def concat_df(s3_uri,df_list,ty,specific_cols):
    try:
        print("S3 Source Concatenating Dataframes")
        if specific_cols[0] in ["*",""]:
            details(s3_uri)
            return pd.concat(df_list,axis=0,ignore_index=True)

        elif specific_cols[0] not in ["","*"] and ty=='json':
            df=pd.concat(df_list,axis=0,ignore_index=True)
            df1=df[specific_cols]

        else:
            df1= pd.concat(df_list, axis=0, ignore_index=True)
            print(df1)

        with open(os.path.join(main_webm.output_dir, 'report.txt'), 'a', newline='') as f:
            f.write(f"S3 Source file Path : {s3_uri}\n")
            f.write(f"S3 Source file Columns : {tuple(df1.columns)}\n")
            f.write(f"S3 Source file Columns count : {df1.shape[1]}\n")
            f.write(f"S3 Source file records count : {df1.shape[0]}\n\n")

        print("S3 file Columns :",tuple(df1.columns))
        print("S3 file Columns count :",df1.shape[1])
        print("S3 file records count :",df1.shape[0],"\n")
        return df1
        
        
    except Exception as e:
        print(e)
        with open(os.path.join(main_webm.output_dir, 'errors.txt'), 'a',newline='') as f:
            f.write(f"\n\n--- Exception S3 Source DF concat {datetime.now().replace(microsecond=0)}---\n{e}")
            main_webm.status="ERROR: [S3 Source concat] Something Went Wrong !!"
        #logging.error("Exception: %s",e)

def details(s3_uri):
    try:
        records:int = 0
        run=True
        columns=[]
        bucket_name=s3_uri.split('/')[2]
        s3_file_path=s3_uri.split(bucket_name)[1][1:]
        #print("--------",s3_file_path)
        #s3_str=f's3://{config.aws_access_key_id}:{config.aws_secret_access_key}@{bucket_name}/{s3_file_path}'
        print("S3 file path: ",f"s3://{bucket_name}/{s3_file_path}")

        theobjects = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=s3_file_path)
        for obj in theobjects['Contents']:
            if obj['Key'].endswith('.csv'):
                #print(obj['Key'])
                if run:    # Get Columns name and Columns count from S3 object
                    resp = s3_client.select_object_content(
                        Bucket=bucket_name,
                        Key=obj['Key'], 
                        ExpressionType='SQL',
                        Expression='''SELECT * FROM s3object limit 1''',
                        #InputSerialization={'CSV': {"FileHeaderInfo": "Use"}, 'CompressionType': 'NONE'}, # Comment this line while reading Parquet file
                        InputSerialization={'CSV': {"FileHeaderInfo": "None"}, 'CompressionType': 'NONE'},
                        OutputSerialization={'CSV': {}},
                    )
                    #print("Connection stablished with s3...")
                    for event in resp['Payload']:
                        if 'Records' in event:
                            cols = event['Records']['Payload'].decode('utf-8')
                            columns=cols.strip().split(',')
                            #print("-----------------------------------------------------------------------\n")
                            print("S3 Source file Columns : ",tuple(columns))
                            print("S3 Source file Columns count: ",len(columns))

                            run=False
                # Get Rows count from S3 object
                
                resp = s3_client.select_object_content(
                    Bucket=bucket_name,
                    Key=obj['Key'], 
                    ExpressionType='SQL',
                    Expression='''SELECT count(*) FROM s3object''',
                    InputSerialization={'CSV': {"FileHeaderInfo": "Use"}, 'CompressionType': 'NONE'}, # Comment this line while reading Parquet file
                    #InputSerialization={'CSV': {"FileHeaderInfo": "None"}, 'CompressionType': 'NONE'},
                    OutputSerialization={'CSV': {}},
                )
                for event in resp['Payload']:
                    if 'Records' in event:
                        records += int(event['Records']['Payload'].decode('utf-8'))
                        #print("-----------------------------------------------------------------------")

            if obj['Key'].endswith('.json'):
        #print(obj['Key'])
                if run:    # Get Columns name and Columns count from S3 object
                    resp = s3_client.select_object_content(
                        Bucket=bucket_name,
                        Key=obj['Key'], 
                        ExpressionType='SQL',
                        Expression='''SELECT * FROM s3object limit 1''',
                        #InputSerialization={'CSV': {"FileHeaderInfo": "Use"}, 'CompressionType': 'NONE'}, # Comment this line while reading Parquet file
                        InputSerialization={'JSON': {"Type": "Document"}},
                        OutputSerialization={'JSON': {}},
                    )
                    #print("Connection stablished with s3...")
                    for event in resp['Payload']:
                        if 'Records' in event:
                            cols = event['Records']['Payload'].decode('utf-8')
                            print(list(json.loads(cols).keys()))
                            columns=list(json.loads(cols).keys())
                            #print("-----------------------------------------------------------------------\n")
                            print("S3 Source file Columns : ",tuple(columns))
                            print("S3 Source file Columns count: ",len(columns))

                            run=False
                # Get Rows count from S3 object
                
                resp = s3_client.select_object_content(
                    Bucket=bucket_name,
                    Key=obj['Key'], 
                    ExpressionType='SQL',
                    Expression='''SELECT count(*) FROM s3object''',
                    InputSerialization={'JSON': {"Type": "Document"}}, # Comment this line while reading Parquet file
                    #InputSerialization={'CSV': {"FileHeaderInfo": "None"}, 'CompressionType': 'NONE'},
                    OutputSerialization={'CSV': {}},
                )
                for event in resp['Payload']:
                    if 'Records' in event:
                        records += int(event['Records']['Payload'].decode('utf-8'))
                        #print("-----------------------------------------------------------------------")

            if obj['Key'].endswith('.parquet'):
                #print(obj['Key'])
                if run:    # Get Columns name and Columns count from S3 object
                    resp = s3_client.select_object_content(
                        Bucket=bucket_name,
                        Key=obj['Key'], 
                        ExpressionType='SQL',
                        Expression='''SELECT * FROM s3object limit 1''',
                        #InputSerialization={'CSV': {"FileHeaderInfo": "Use"}, 'CompressionType': 'NONE'}, # Comment this line while reading Parquet file
                        InputSerialization={'Parquet': {}},
                        OutputSerialization={'JSON': {}},
                    )
                    #print("Connection stablished with s3...")
                    for event in resp['Payload']:
                        if 'Records' in event:
                            cols = event['Records']['Payload'].decode('utf-8')
                            print(list(json.loads(cols).keys()))
                            columns=list(json.loads(cols).keys())
                            #columns=cols.split(',')
                            #print("-----------------------------------------------------------------------\n")
                            print("S3 Source file Columns : ",tuple(columns))
                            print("S3 Source file Columns count: ",len(columns))

                            run=False
                # Get Rows count from S3 object
                
                resp = s3_client.select_object_content(
                    Bucket=bucket_name,
                    Key=obj['Key'], 
                    ExpressionType='SQL',
                    Expression='''SELECT count(*) FROM s3object''',
                    InputSerialization={'Parquet': {}}, # Comment this line while reading Parquet file
                    #InputSerialization={'CSV': {"FileHeaderInfo": "None"}, 'CompressionType': 'NONE'},
                    OutputSerialization={'CSV': {}},
                )
                for event in resp['Payload']:
                    if 'Records' in event:
                        records += int(event['Records']['Payload'].decode('utf-8'))
                        #print("-----------------------------------------------------------------------")

        print("S3 Source file records count: -",records)
        with open(os.path.join(main_webm.output_dir, 'report.txt'), 'a', newline='') as f:
            f.write(f"S3 Source file Path : {s3_uri}\n")
            f.write(f"S3 Source file Columns : {tuple(columns)}\n")
            f.write(f"S3 Source file Columns count : {len(columns)}\n")
            f.write(f"S3 Source file records count : {records}\n\n")



    except Exception as e:
        print(e)
        with open(os.path.join(main_webm.output_dir, 'errors.txt'), 'a',newline='') as f:
            f.write(f"\n\n--- Exception S3 Source detail {datetime.now().replace(microsecond=0)}---\n{e}")
            main_webm.status="[S3 Source Details] Something Went Wrong !!"


#print(details("s3://ideakafkatest/QA_UI_testing/ftp/6/oracle/C#DEMO_USER/INVENTORIES/load_date=23-02-2022/load_time=135122/part-00000-f50a5d21-bfa3-4e2b-8338-752e4282dca3-c000.snappy.parquet"))

# print(dataframe("s3://ideakafkatest/QA_UI_testing/ftp/4/oracle/C#DEMO_USER/INVENTORIES/load_date=23-02-2022/load_time=151027/part-00000-7d23595b-f711-498a-b771-e631f8359108-c000.json"))
#print(details("s3://ideakafkatest/QA_UI_testing/ftp/3/sftp/sftpuser1/demo-data/facts/INVENTORIES_INDEX.csv"))






# def dataframe_s3(s3_uri):
#     try:
#         bucket_name=s3_uri.split('/')[2]
#         s3_file_path=s3_uri.split(bucket_name)[1][1:]
#         s3_str=f's3://{config.aws_access_key_id}:{config.aws_secret_access_key}@{bucket_name}/{s3_file_path}'
#         print("S3 file path: ",f"s3://{bucket_name}/{s3_file_path}")
#     except Exception as e:
#         print(e)

#     # Get Columns name and Columns count from S3 object
#     try:
#         resp = s3_client.select_object_content(
#             Bucket=bucket_name,
#             Key=s3_file_path, 
#             ExpressionType='SQL',
#             Expression='''SELECT * FROM s3object limit 1''',
#             #InputSerialization={'CSV': {"FileHeaderInfo": "Use"}, 'CompressionType': 'NONE'}, # Comment this line while reading Parquet file
#             InputSerialization={'CSV': {"FileHeaderInfo": "None"}, 'CompressionType': 'NONE'},
#             OutputSerialization={'CSV': {}},
#         )
#         #print("Connection stablished with s3...")
#         for event in resp['Payload']:
#             if 'Records' in event:
#                 records = event['Records']['Payload'].decode('utf-8')
#                 columns=records.split(',')
#                 #print("-----------------------------------------------------------------------\n")
#                 print("S3 Sink file Columns : ",tuple(columns))
#                 print("S3 Sink file Columns count: ",len(columns))

#     except Exception as e:
#             print(e)

#     # Get Rows count from S3 object
#     try:
#         resp = s3_client.select_object_content(
#             Bucket=bucket_name,
#             Key=s3_file_path, 
#             ExpressionType='SQL',
#             Expression='''SELECT count(*) FROM s3object''',
#             InputSerialization={'CSV': {"FileHeaderInfo": "Use"}, 'CompressionType': 'NONE'}, # Comment this line while reading Parquet file
#             #InputSerialization={'CSV': {"FileHeaderInfo": "None"}, 'CompressionType': 'NONE'},
#             OutputSerialization={'CSV': {}},
#         )
#         for event in resp['Payload']:
#             if 'Records' in event:
#                 records = event['Records']['Payload'].decode('utf-8')
#                 #print("-----------------------------------------------------------------------")
#                 print("S3 Sink file records count: ",records)
#     except Exception as e:
#             print(e)        
#     return pd.read_csv(smart_open(s3_str))#,nrows=chunks)



# other method using pandas and io

# def dataframe_s3(s3_uri):
#     bucket_name=s3_uri.split('/')[2]
#     s3_file_path=s3_uri.split(bucket_name)[1][1:]
#     try:
#         #'test/file2.csv'
#         resp=s3_client.get_object(Bucket=bucket_name,Key=s3_file_path)
#         status = resp.get("ResponseMetadata", {}).get("HTTPStatusCode")
#         if status == 200:
#             print("Getting S3 object, Please wait... ")
#             return pd.read_csv(io.BytesIO(resp['Body'].read()))
#         else:
#             print(f"Unsuccessful S3 get_object response. Status - {status}")
#     except Exception as e:
#         print(e)

#print(dataframe("s3://ideakafkatest/idea-cdf-ftp-20211209064253705/ftp/synthetic_data_200_000-10.csv"))