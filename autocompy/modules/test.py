import pandas as pd

#df=pd.read_csv(f"ftp://{config.ftp_username}:{config.ftp_password}@{config.ftp_host}:{config.ftp_port}/idea_demo/synthetic_data_200_000.csv",usecols=[0])
#print(df.head())
# #logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s",filename='Output/logs.log', encoding='utf-8', level=logging.DEBUG)
# # Get Column names, Columns count, rows count and return Dataframe from S3 object
# s3_client = boto3.client(
#                 service_name=config.service_name,
#                 region_name=config.region_name,
#                 aws_access_key_id=config.aws_access_key_id,
#                 aws_secret_access_key=config.aws_secret_access_key)

# s3_uri="s3://ideakafkatest/QA_UI_testing/ftp/6/oracle/C#DEMO_USER/INVENTORIES/load_date=23-02-2022/load_time=135122/part-00000-f50a5d21-bfa3-4e2b-8338-752e4282dca3-c000.snappy.parquet"

# records:int = 0
# run=True
# columns=[]
# bucket_name=s3_uri.split('/')[2]
# s3_file_path=s3_uri.split(bucket_name)[1][1:]
# #print("--------",s3_file_path)
# #s3_str=f's3://{config.aws_access_key_id}:{config.aws_secret_access_key}@{bucket_name}/{s3_file_path}'
# print("S3 file path: ",f"s3://{bucket_name}/{s3_file_path}")

# theobjects = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=s3_file_path)
# for obj in theobjects['Contents']:
#     if obj['Key'].endswith('.csv'):
#         #print(obj['Key'])
#         if run:    # Get Columns name and Columns count from S3 object
#             resp = s3_client.select_object_content(
#                 Bucket=bucket_name,
#                 Key=obj['Key'], 
#                 ExpressionType='SQL',
#                 Expression='''SELECT * FROM s3object limit 1''',
#                 #InputSerialization={'CSV': {"FileHeaderInfo": "Use"}, 'CompressionType': 'NONE'}, # Comment this line while reading Parquet file
#                 InputSerialization={'CSV': {"FileHeaderInfo": "None"}, 'CompressionType': 'NONE'},
#                 OutputSerialization={'CSV': {}},
#             )
#             #print("Connection stablished with s3...")
#             for event in resp['Payload']:
#                 if 'Records' in event:
#                     cols = event['Records']['Payload'].decode('utf-8')
#                     columns=cols.split(',')
#                     #print("-----------------------------------------------------------------------\n")
#                     print("S3 Sink file Columns : ",tuple(columns))
#                     print("S3 Sink file Columns count: ",len(columns))

#                     run=False
#         # Get Rows count from S3 object
        
#         resp = s3_client.select_object_content(
#             Bucket=bucket_name,
#             Key=obj['Key'], 
#             ExpressionType='SQL',
#             Expression='''SELECT count(*) FROM s3object''',
#             InputSerialization={'CSV': {"FileHeaderInfo": "Use"}, 'CompressionType': 'NONE'}, # Comment this line while reading Parquet file
#             #InputSerialization={'CSV': {"FileHeaderInfo": "None"}, 'CompressionType': 'NONE'},
#             OutputSerialization={'CSV': {}},
#         )
#         for event in resp['Payload']:
#             if 'Records' in event:
#                 records += int(event['Records']['Payload'].decode('utf-8'))
#                 #print("-----------------------------------------------------------------------")

#     if obj['Key'].endswith('.json'):
#         #print(obj['Key'])
#         if run:    # Get Columns name and Columns count from S3 object
#             resp = s3_client.select_object_content(
#                 Bucket=bucket_name,
#                 Key=obj['Key'], 
#                 ExpressionType='SQL',
#                 Expression='''SELECT * FROM s3object limit 1''',
#                 #InputSerialization={'CSV': {"FileHeaderInfo": "Use"}, 'CompressionType': 'NONE'}, # Comment this line while reading Parquet file
#                 InputSerialization={'JSON': {"Type": "Document"}},
#                 OutputSerialization={'JSON': {}},
#             )
#             #print("Connection stablished with s3...")
#             for event in resp['Payload']:
#                 if 'Records' in event:
#                     cols = event['Records']['Payload'].decode('utf-8')
#                     print(list(json.loads(cols).keys()))
#                     columns=list(json.loads(cols).keys())
#                     #print("-----------------------------------------------------------------------\n")
#                     print("S3 Sink file Columns : ",tuple(columns))
#                     print("S3 Sink file Columns count: ",len(columns))

#                     run=False
#         # Get Rows count from S3 object
        
#         resp = s3_client.select_object_content(
#             Bucket=bucket_name,
#             Key=obj['Key'], 
#             ExpressionType='SQL',
#             Expression='''SELECT count(*) FROM s3object''',
#             InputSerialization={'JSON': {"Type": "Document"}}, # Comment this line while reading Parquet file
#             #InputSerialization={'CSV': {"FileHeaderInfo": "None"}, 'CompressionType': 'NONE'},
#             OutputSerialization={'CSV': {}},
#         )
#         for event in resp['Payload']:
#             if 'Records' in event:
#                 records += int(event['Records']['Payload'].decode('utf-8'))
#                 #print("-----------------------------------------------------------------------")

#     if obj['Key'].endswith('.parquet'):
#         #print(obj['Key'])
#         if run:    # Get Columns name and Columns count from S3 object
#             resp = s3_client.select_object_content(
#                 Bucket=bucket_name,
#                 Key=obj['Key'], 
#                 ExpressionType='SQL',
#                 Expression='''SELECT * FROM s3object limit 1''',
#                 #InputSerialization={'CSV': {"FileHeaderInfo": "Use"}, 'CompressionType': 'NONE'}, # Comment this line while reading Parquet file
#                 InputSerialization={'Parquet': {}},
#                 OutputSerialization={'JSON': {}},
#             )
#             #print("Connection stablished with s3...")
#             for event in resp['Payload']:
#                 if 'Records' in event:
#                     cols = event['Records']['Payload'].decode('utf-8')
#                     print(list(json.loads(cols).keys()))
#                     columns=list(json.loads(cols).keys())
#                     #columns=cols.split(',')
#                     #print("-----------------------------------------------------------------------\n")
#                     print("S3 Sink file Columns : ",tuple(columns))
#                     print("S3 Sink file Columns count: ",len(columns))

#                     run=False
#         # Get Rows count from S3 object
        
#         resp = s3_client.select_object_content(
#             Bucket=bucket_name,
#             Key=obj['Key'], 
#             ExpressionType='SQL',
#             Expression='''SELECT count(*) FROM s3object''',
#             InputSerialization={'Parquet': {}}, # Comment this line while reading Parquet file
#             #InputSerialization={'CSV': {"FileHeaderInfo": "None"}, 'CompressionType': 'NONE'},
#             OutputSerialization={'CSV': {}},
#         )
#         for event in resp['Payload']:
#             if 'Records' in event:
#                 records += int(event['Records']['Payload'].decode('utf-8'))
#                 #print("-----------------------------------------------------------------------")
#     print("S3 Sink file Rows count: ",records)

# with teradatasql.connect(host=config.td_host, user=config.td_username, password=config.td_password) as connect:      
#    df1 = pd.read_sql(f'select system_id_1 from IDEA_QA_TEST_DB.synthetic_data_200_000;', connect)
#    print(df1.head())

# ftp=ftplib.FTP()
# ftp.connect(config.ftp_host,int(config.ftp_port))
# ftp.login(config.ftp_username,config.ftp_password)

# file=io.BytesIO()

# conn=ftp.transfercmd("RETR {ftp_file_path}")
# binary_file=conn.makefile('rb')
# count=0
# while count<1:
#    line=binary_file.readline(ftp.maxline + 1)
#    file.write(line)
#    count+=1

# binary_file.close()
# conn.close()
# # ftp.quit()

# cols=str(file.getvalue())

# print(cols)


# import pandas as pd
# import ftplib, io
# path="ftp://pfsuftp:ftpserver123@137.117.77.235:24565/idea_demo/synthetic_data_200_000.csv"
# #df1=pd.read_csv(path, usecols=[0])
# #df=pd.read_csv(path,nrows=1)

# #print(df1.count())

# ftp = ftplib.FTP()
# ftp.connect('137.117.77.235', 24565)
# ftp.login('pfsuftp','ftpserver123')
# file=io.BytesIO()


# #server.sendcmd('TYPE A')
# conn = ftp.transfercmd("RETR {}".format("/idea_demo/synthetic_data_200_000-1.csv"))
# b_file = conn.makefile('rb')
# count = 0
# while count <1:
#     line = b_file.readline(ftp.maxline + 1)
#     file.write(line)
#     count += 1


# #print(pd.read_csv(fp))
# b_file.close()
# conn.close()

# print("  -> "+str(file.getvalue())[2:])
#print(str(download_file,'utf-8'))
#print(pd.read_csv(fp)
# path="sftp://pfsu:%s@20.121.72.104:22//home/pfsu/cdf_data/facts/INVENTORIES_INDEX.csv"%quote(config.sftp_password)
# path1=f"sftp://{config.sftp_username}:{config.sftp_password}@{config.sftp_host}:{config.sftp_port}/home/pfsu/cdf_data/facts/INVENTORIES_INDEX.csv"
# print(path)

# if path.endswith('.csv'):
#    df1=pd.read_csv(smart_open(path), usecols=[0])  
# if path.endswith('.txt'):
#    df1=pd.read_csv(smart_open(path),sep='¬',engine='python', usecols=[0])

# if path.endswith('.csv'):
#    df_col=pd.read_csv(smart_open(path), nrows=1)  
# if path.endswith('.txt'):
#    df_col=pd.read_csv(smart_open(path),sep='¬',engine='python', nrows=1)



# print("Sftp file Columns :",tuple(df1.columns))
# print("Sftp file Columns count :",df1.shape[1])
# print("Sftp file records count :",df1.shape[0],"\n")
# print("Sort Summary :", df1.info(),"\n")


# with teradatasql.connect(host=config.td_host, user=config.td_username,
#                          password=config.td_password) as connection:
#
#     df1 = pd.read_sql(f'''SELECT  DatabaseName,
#         TableName,
#         CreateTimeStamp,
#         LastAlterTimeStamp
# FROM    DBC.TablesV
# WHERE   TableKind = 'T'
# ORDER BY    DatabaseName,
#             TableName;''', connection)
#
#     print(df1)


df=pd.read_csv("inputs/test.csv", sep="\s+|;|:|\t|,", engine="python")
print(df.head())




