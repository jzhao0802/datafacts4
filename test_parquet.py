import time
import datetime
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.readwriter import *
from pyspark.sql.functions import *
import unittest

sc = SparkContext(appName = 'parquet_test')
sqlContext = SQLContext(sc)


numRowsReq = 100000000

pos_file = "ipf_sample.csv"
neg_file = "nonipf_ac_sample.csv"

start_time = time.time()
st = datetime.datetime.fromtimestamp(start_time).strftime('%Y%m%d_%H%M%S')

out_file = "task9_" +st+".parquet"
out_datafactz = "task9_datafactz_"+st+".parquet"

data_path = "s3://emr-rwes-pa-spark-dev-datastore/BI_IPF_2016/01_data/"
output_path = "s3://emr-rwes-pa-spark-dev-datastore/BI_IPF_2016/02_results/"


class ParquetTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        pos = sqlContext.read.load((data_path + pos_file),
                          format='com.databricks.spark.csv',
                          header='true',
                          inferSchema='true')

        neg = sqlContext.read.load((data_path + neg_file),
                          format='com.databricks.spark.csv',
                          header='true',
                          inferSchema='true')

        dataColumns = pos.columns
        cls.data = pos.select(dataColumns).unionAll(neg.select(dataColumns))
        cls.data.write.parquet(output_path + out_file)

        

    def test_is_read_working(self):
        self.assertTrue(ParquetTests.data.count() > 1, "There is a problem in reading the document")

    def test_is_write_actual_data_working(self):
        ims_data = sqlContext.read.parquet(output_path + out_file)
        subtractedSet = ims_data.subtract(ParquetTests.data)
        self.assertEqual(subtractedSet.count(), 0, "Data mismatch. Actual data is not same as the input.")

    def test_is_write_simulated_data_working(self):
        df = sqlContext.range(0, numRowsReq)
        datafactz_df = df.select(rand().alias("Col1"), rand().alias("Col2"), rand().alias("Col3"))
        datafactz_df.write.parquet(output_path + out_datafactz)
        datafactz_data = sqlContext.read.parquet(output_path + out_datafactz)
        self.assertEqual(datafactz_data.count(), numRowsReq, "Data mismatch. Simulated data is not same as the input.")

if __name__ == "__main__":
    unittest.main()

