import time
import datetime
from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext, DataFrameWriter
from pyspark.sql.functions import rand

sc = SparkContext(appName = 'hive_test')
sqlContext = HiveContext(sc)

numRowsReq = 100000000

pos_file = "ipf_sample.csv"
neg_file = "nonipf_ac_sample.csv"
out_file = "task9.hive"
out_datafactz = "task9_datafactz.hive"
data_path = "s3://emr-rwes-pa-spark-dev-datastore/BI_IPF_2016/01_data/"
output_path = "s3://emr-rwes-pa-spark-dev-datastore/BI_IPF_2016/02_results/"
start_time = time.time()
st = datetime.datetime.fromtimestamp(start_time).strftime('%Y%m%d_%H%M%S')

table_name = "hive_test_" + st
datafactz_table_name = "hive_test_datafactz_" + st

pos = sqlContext.read.load((data_path + pos_file),
                          format='com.databricks.spark.csv',
                          header='true',
                          inferSchema='true')

neg = sqlContext.read.load((data_path + neg_file),
                          format='com.databricks.spark.csv',
                          header='true',
                          inferSchema='true')

dataColumns = pos.columns

data = pos.select(dataColumns).unionAll(neg.select(dataColumns))
data.write.save(path= output_path + table_name, format='orc')

df = sqlContext.range(0, numRowsReq).repartition(20)

datafactz_df = df.select(rand().alias("Col1"), rand().alias("Col2"), rand().alias("Col3"))

datafactz_df.write.save(path= output_path + datafactz_table_name, format='orc')

