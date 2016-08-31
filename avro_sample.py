import time
import datetime
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.readwriter import *

sc = SparkContext(appName = 'avro_test')
sqlContext = SQLContext(sc)


numRowsReq = 100000000

pos_file = "ipf_sample.csv"
neg_file = "nonipf_ac_sample.csv"

start_time = time.time()
st = datetime.datetime.fromtimestamp(start_time).strftime('%Y%m%d_%H%M%S')

out_file = "task9_" + st+".avro"
out_datafactz = "task9_datafactz_" +st +".avro"
data_path = "s3://emr-rwes-pa-spark-dev-datastore/BI_IPF_2016/01_data/"
output_path = "s3://emr-rwes-pa-spark-dev-datastore/BI_IPF_2016/02_results/"

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

#for IMS
data.write.format("com.databricks.spark.avro").save(output_path + out_file)

#for datafactz
df = sqlContext.range(0, numRowsReq)
datafactz_df = df.select(rand().alias("Col1"), rand().alias("Col2"), rand().alias("Col3"))
datafactz_df.write.format("com.databricks.spark.avro").save(output_path + out_datafactz)

