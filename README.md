# cdc-consumer
Change Data Capture Consumer


Load Parquet files on Hiven
Connect to docker instanc e

`docker exec -it hiveserver2 beeline -u 'jdbc:hive2://localhost:10000/'`

-- Load all partitions Hive doesn't know about yet
MSCK REPAIR TABLE audit_log;



