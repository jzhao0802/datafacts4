import time
import datetime
from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext, DataFrameWriter
from pyspark.sql.functions import rand
import unittest

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


class HiveTests(unittest.TestCase):
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
        cls.data.write.save(path= output_path + table_name, format='orc')



    def test_is_read_working(self):
        self.assertTrue(HiveTests.data.count() > 1, "There is a problem in reading the document")

    def test_is_write_actual_data_working(self):
        ims_data = sqlContext.read.format("orc").load(output_path + table_name)
        subtractedSet = ims_data.subtract(HiveTests.data)
        self.assertEqual(subtractedSet.count(), 0, "Data mismatch. Actual data is not same as the input.")

    def test_is_write_simulated_data_working(self):
        df = sqlContext.range(0, numRowsReq)
        datafactz_df = df.select(rand().alias("Col1"), rand().alias("Col2"), rand().alias("Col3"))
        datafactz_df.write.save(path= output_path + datafactz_table_name, format='orc')
        datafactz_data = sqlContext.read.format("orc").load(output_path + datafactz_table_name)
        self.assertEqual(datafactz_data.count(), numRowsReq, "Data mismatch. Simulated data is not same as the input.")


if __name__ == "__main__":
    unittest.main()

