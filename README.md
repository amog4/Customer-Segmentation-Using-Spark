# Segmentation by using PySpark (This is an end to end Data engineering project solution) 


This is an end to end solution of RFM using pyspark it takes input in avro format and gives aggregations on customer level and also on segmentation level too.

what input parameters does python/main.py takes
  1. file_path (avro file path)
  2. days or start_date and end_date
  3. dict_columns (track_id -  customer or transaction id, organization,amount,tranx_date,product,orders,country,customer_type )
  
In the output we get latency,ABS,total recency , total frequency , total monetary value with segments per customer further we also get segmentation level inputs 
output dir can be added manually. 

This automated segmentation can run real time, if we have a spark cluster setup.

