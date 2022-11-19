import os
from configparser import ConfigParser


class Config:
    SECRET_KEY = '65sifu12y49ycwdhc7r27992040c12wciuerh74ry1221'
    SQLALCHEMY_DATABASE_URI = 'mysql://root:root@localhost/autocompy'
    MAIL_SERVER = 'smtp.googlemail.com'
    MAIL_PORT = 587
    MAIL_USE_TLS = True
    MAIL_USERNAME = 'scaninfoaskme@gmail.com'
    MAIL_PASSWORD = 'hekzocfuplptvvlv'


file = 'autocompy\.init\config.ini'
config = ConfigParser()
config.read(file)

# s3
service_name = config["s3"]["service_name"]
region_name = config["s3"]["region_name"]
aws_access_key_id = config["s3"]["aws_access_key_id"]
aws_secret_access_key = config["s3"]["aws_secret_access_key"]

# ftp
ftp_host = config["ftp"]["hostname"]
ftp_port = config["ftp"]["port"]
ftp_username = config["ftp"]["username"]
ftp_password = config["ftp"]["password"]

# sftp
sftp_host = config["sftp"]["hostname"]
sftp_port = config["sftp"]["port"]
sftp_username = config["sftp"]["username"]
sftp_password = config["sftp"]["password"]

# oracle
o_hostname = config["oracle"]["hostname"]
o_port = config["oracle"]["port"]
o_username = config["oracle"]["username"]
o_password = config["oracle"]["password"]
o_sid = config["oracle"]["sid"]

# snowflake
snow_host = config["snow-mvp"]["hostname"]
snow_username = config["snow-mvp"]["username"]
snow_password = config["snow-mvp"]["password"]

snowqa_host = config["snow-qa"]["hostname"]
snowqa_username = config["snow-qa"]["username"]
snowqa_password = config["snow-qa"]["password"]

# sql server
ss_server = config["sqlserver"]["server"]
ss_username = config["sqlserver"]["username"]
ss_password = config["sqlserver"]["password"]
ss_database = config["sqlserver"]["database"]

# redshift
red_host = config["redshift"]['hostname']
red_username = config['redshift']['username']
red_password = config["redshift"]['password']
red_port = config["redshift"]['port']
red_db = config["redshift"]['db_name']

# teradata
td_host = config["teradata"]["hostname"]
td_port = config["teradata"]["port"]
td_username = config["teradata"]["username"]
td_password = config["teradata"]["password"]

# mysql
m_host = config["mysql"]["hostname"]
m_port = config["mysql"]["port"]
m_username = config["mysql"]["username"]
m_password = config["mysql"]["password"]
m_database = config["mysql"]["database"]
