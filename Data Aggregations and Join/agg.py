from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark import SparkConf
conf = SparkConf()\
      .setMaster('local')\
      .setAppName('AggDemo')
spark = SparkSession.builder.config(conf=conf).getOrCreate()
sc = spark.sparkContext
DATASET_PATH = 'drive/MyDrive/.../invoices.csv'

invoice_df = spark.read\
            .option('header','true')\
            .option('inferSchema', 'true')\
            .csv(DATASET_PATH)
invoice_df.show()

invoice_df.select(count('*'),
                  sum('Quantity').alias('TotalQuantity'),
                  avg('UnitPrice').alias('AvgPrice'),
                  countDistinct('InvoiceNo').alias('CountDistinct')
                  ).show()

invoice_df.createOrReplaceTempView('invoice_tb') # tạo bảng tạm
summary_sql = spark.sql("""
              select
                Country,
                InvoiceNo,
                sum(Quantity) as TotalQuantity,
                round(sum(Quantity*UnitPrice), 2)  as InvoiceValue
              from invoice_tb
              group by Country, InvoiceNo
""")

summary_sql.show()
