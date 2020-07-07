import sys
import configparser  as cp
from pyspark.sql import SparkSession
import cassandra_cqlshfile
import mysql_class
import pandas as pd
import rfm_class
import rfm_tables
import pyspark.sql.functions as F

props = cp.RawConfigParser()
props.read('src/main/resources/application.properties')
env = sys.argv[1]
#csv = sys.argv[2]



cassandra_connection_host = props.get(env,'cassandra_connection_host')
cassandra_connection_port = props.get(env,'cassandra_conncection_port')
cassandra_key_space_name = props.get(env,'cassandra_key_space_name')
cassandra_user = props.get(env,'cassandra_user')
cassandra_password = props.get(env,'cassandra_password')
cassandra_insert_track  = props.get(env,'cassandra_insert_track')
#cassandra_insert_track_rfm = props.get(env,'cassandra_insert_track_rfm')
cassandra_sql_owners_info = props.get(env,'cassandra_sql_owners_info')
cassandra_main_table = props.get(env,'table_name')




spark = SparkSession.builder \
        .master(props.get(env, 'executionMode')) \
        .config("spark.driver.memory",'15g')\
        .config("spark.sql.codegen.aggregate.map.twolevel.enabled",True)\
        .appName('RFManlytics') \
        .getOrCreate()





spark.stop()


"""r'555': 'Champions',
r'43[4-5]': 'Loyal',
r'53[3-5]': 'Loyal',
r'4[4-5][3-5]': 'Loyal',
r'5[4-5][3-4]': 'Loyal',
r'5[1-2][4-5]': 'New',
r'545': 'New',
r'4[1-2][4-5]': 'Recent',
r'433': 'Recent',
r'[4-5][3-5]2': 'Potential Loyalist',
r'3[3-5][2-5]': 'Potential Loyalist',
r'3[1-2][3-5]': 'Faithful',
r'[4-5][4-5]1': 'Faithful',
r'[4-5][1-2]3': 'Faithful',
r'[4-5][1-2][1-2]': 'Price Sensitive',
r'[4-5]31': 'Price Sensitive',
r'11[4-5]': 'Cant Lose Them',
r'1[2-5][3-5]': 'Hibernating',
r'113': 'Hibernating',
r'1[1-5][1-2]': 'Lost',
r'2[1-5][1-5]': 'About To Sleep',
r'3[1-3][1-2]': 'About To Sleep'

spark-submit --packages org.apache.spark:spark-avro_2.11:2.4.4 --conf avro.mapred.ignore.inputs.without.extension=True,spark.sql.cbo.enabled=True src/main/python/main.py dev days=364 file_path='/home/csai/Documents/av/file.avro' dict_columns='{"customer_id":"track_id","ownername":"organization","transaction_date":"tranx_date", "transaction_amount":"amount"}' """


#spark-submit --packages org.apache.spark:spark-avro_2.11:2.4.4 --conf avro.mapred.ignore.inputs.without.extension=True,spark.sql.cbo.enabled=True src/main/python/main.py dev start_date='2019-10-01' end_date='2019-12-31' file_path='/home/csai/Documents/av/transactions_avro/*.avro' dict_columns='{"customer_id":"track_id","tranx_date":"tranx_date", "gross_amount":"amount","quantity":"orders"}' """
