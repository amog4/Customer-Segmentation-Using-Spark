# Customer Segmentation by using PySpark (This is an end to end Data engineering project solution) 


├── src
│   ├── main
│     ├── python
          ├── main.py
│     ├──input_features_modules
│     ├── read_write_modules
│     ├── transform_modules
      ├── rfm_modules
      ├── logger_module


This is an end to end solution of RFM using pyspark it takes input in avro format and gives aggregations on customer level and also segmentation level too.

Parameters does python/main.py takes
  1. file_path (avro file path)
  2. days or start_date and end_date
  3. dict_columns (track_id -  customer or transaction id, organization,amount,tranx_date,product,orders,country,customer_type)
  
In the output we get latency,ABS,total recency , total frequency , total monetary value with segments per customer further we also get segmentation level inputs 
output dir to be added in resource application properties file. 

This automated segmentation can run in real time.

spark-submit --packages org.apache.spark:spark-avro_2.11:2.4.4 --conf avro.mapred.ignore.inputs.without.extension=True,spark.sql.cbo.enabled=True src/main/python/main.py dev start_date= end_date= file_path= dict_columns='' 


