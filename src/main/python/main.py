import os
import sys
sys.path.insert(1, 'src/main')
import configparser  as cp
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *

import rfm_modules.rfm_functions as rfm_class
import rfm_modules.rfm_agg as rfm_agg
import read_write_modules.read as read_tables
import read_write_modules.write as write_tables
import input_features_modules.input_features as inputs
import transform_modules.transform as transforms
from logger_module.logger_function import setup_logger
import logging
import time
import json


log_info = setup_logger(name ='info', log_file = 'var/log/dev_info.log', level=logging.INFO)
log_error = setup_logger(name ='error', log_file = 'var/log/dev_error.log', level=logging.ERROR)
#log_format = '%(asctime)s %(filename)s: %(message)s'
#logging.basicConfig(filename='test1.log', format=log_format)

#from kafka.admin import KafkaAdminClient, NewTopic

#log_format = '%(asctime)s %(filename)s: %(message)s'
#logging.basicConfig(filename='rfm.log', format=log_format)


props = cp.RawConfigParser()
props.read('src/main/resources/application.properties')
env = sys.argv[1]

inputs_obj = inputs.Input()
read_tables_obj = read_tables.Read_module()
transforms_obj = transforms.TransformFeatures()
write_obj = write_tables.Write_module()


start_time = time.time()
#columns_track = ['uid', 'inserted_date', 'topic', 'offset']


def main(file_path,days,start_date,end_date,dict_columns):
    log_info.info("Program started")
    try:
        spark = SparkSession.builder \
            .appName('streaming_csv') \
            .getOrCreate()

        log_info.info("Spark object created")


        dict_columns_ =  json.loads('{"customer_id":"track_id","tranx_date":"tranx_date","gross_amount":"amount","quantity":"orders"}')
        print(dict_columns_)




        col_names,data_columns = inputs_obj.input_features(dict_columns = dict_columns_)

        data_types = inputs_obj.input_datatypes(col_names = col_names)



        transactions_detail_csv = read_tables_obj.read_avro_file(file_path = file_path,file_format ='avro',spark=spark)




        transactions_detail_csv = transforms_obj.read_avro_transform(data_columns = data_columns,
                                                                     df = transactions_detail_csv
                                            ,data_types = data_types,
                                            dict_columns = dict_columns_)




        transactions_detail_csv.createOrReplaceTempView('rfm_table')


        start_end_date = transforms_obj.find_start_end_date(days = days,spark =spark,start_date =start_date,end_date =end_date)





        rfm_object = rfm_class.Transform(df=transactions_detail_csv,
                                         start_date_end_date=start_end_date,
                                         spark=spark,
                                         columns = col_names)
        data = rfm_object.transformation()


        #data = rfm_object.transformation()

        rfm_aggregations = rfm_agg.rfm_agg()
        data_agg = rfm_aggregations.rfm_(data=data, spark=spark, columns=col_names)
    except Exception as e:
        log_error.error(e, exc_info=True)


    try:
        write_obj.write_csv(df = data ,mode ='append',path = '/E:/linux/backup/Documents/og_tranx/customer_level_test/rfm_product_20191001-20191231/')
    except Exception as e:
        log_error.error(e, exc_info=True)
    try:
        write_obj.write_csv(df=data_agg, mode='append',
                        path='/E:/linux/backup/Documents/og_tranx/customer_level_test/rfm_product_20191001-20191231/')
    except Exception as e:
        log_error.error(e, exc_info=True)



    log_info.info( " Program ended & it took --- %s seconds ---" % (time.time() - start_time))
    spark.stop()



if __name__ == "__main__":
    props = cp.RawConfigParser()
    props.read('src/main/resources/application.properties')
    env = sys.argv[1]


    def load_data(*argv, **kwargs):
        return kwargs
        #


    parameters = load_data(**dict([ar.split('=') for ar in sys.argv[1:] if ar.find('=') > 0]))

    # print((parameters.get('monthly',None)))
    # print(parameters.get('predictionfile',None))
    main(
        file_path=parameters.get('file_path', None),
        days=parameters.get('days', None),
        start_date=parameters.get('start_date', None),
        end_date=parameters.get('end_date', None),
        dict_columns=parameters.get('dict_columns', None)
    )

