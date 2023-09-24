from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark import SparkConf
from pyspark.sql import Window
conf = SparkConf() \
    .setMaster('local') \
    .setAppName('WindowingDemo')

spark = SparkSession.builder.config(conf=conf).getOrCreate()
sc = spark.sparkContext

DATASET_PATH = 'drive/MyDrive/.../summary.parquet'

summary_df = spark.read.parquet(DATASET_PATH)
summary_df.printSchema()

running_total_window = Window.partitionBy('Country')\  # tạo cửa sổ trượt
                             .orderBy('WeekNumber')\
                             .rowsBetween(Window.unboundedPreceding, Window.currentRow)

summary_df.withColumn("RunningTotal",sum('InvoiceValue').over(running_total_window)) \  # tính tổng theo cửa sổ - tổng tích lũy
    .show()
