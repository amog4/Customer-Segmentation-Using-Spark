# Customer Segmentation by using PySpark 

* This repository is an end to end solution of RFM using pyspark it takes input in avro format and gives aggregations on customer level and also segmentation level too.

## python/main.py takes following Parameters:
  1. file_path (avro file path)
  2. days or start_date and end_date
  3. dict_columns (track_id -  customer or transaction id, organization,amount,tranx_date,product,orders,country,customer_type)
  
## Output dir to be added in resource application properties file
  
  1. We get latency,ABS,total recency , total frequency , total monetary value with segments per customer further we also get segmentation level outputs.
 
## Terminal Command :- 
  * spark-submit --packages org.apache.spark:spark-avro_2.11:2.4.4 --conf avro.mapred.ignore.inputs.without.extension=True,spark.sql.cbo.enabled=True src/main/python/main.py dev     start_date= end_date= file_path= dict_columns='' 


