# Bài này mình làm trên ggcolab
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DateType, StringType, IntegerType
from pyspark.sql.functions import spark_partition_id

conf = SparkConf() \
    .setMaster('local') \
    .setAppName('lap3')

spark = SparkSession.builder.config(conf=conf).getOrCreate()
sc = spark.sparkContext
DATASET_PATH = 'drive/MyDrive/.../flight-time.csv' # ghi đường dẫn vào đây

flightSchemaStruct = StructType([
  StructField('FL_DATE', DateType()),
  StructField('OP_CARRIER', StringType()),
  StructField('OP_CARRIER_FL_NUM', IntegerType()),
  StructField('ORIGIN', StringType()),
  StructField('ORIGIN_CITY_NAME', StringType()),
  StructField('DEST', StringType()),
  StructField('DEST_CITY_NAME', StringType()),
  StructField('CRS_DEP_TIME', IntegerType()),
  StructField('DEP_TIME', IntegerType()),
  StructField('WHEELS_ON', IntegerType()),
  StructField('TAXI_IN', IntegerType()),
  StructField('CRS_ARR_TIME', IntegerType()),
  StructField('ARR_TIME', IntegerType()),
  StructField('CANCELLED', IntegerType()),
  StructField('DISTANCE', IntegerType()),
])

print('Schema by StructType')
flightTimeCsvDF = spark.read \
    .format('csv') \
    .option("header", 'true') \
    .schema(flightSchemaStruct) \
    .option("mode", "FAILFAST") \  # đưa ra lỗi cho hệ thống khi có hàng bị lỗi 
    .option("dateFormat", 'M/d/y') \  # format lại để spark có thể đọc được
    .load(DATASET_PATH)

flightTimeCsvDF.show(5)

# Kiểm tra số lượng phân vùng và số record của từng phân vùng 
flightTimeCsvDF.groupBy(spark_partition_id()).count().show()
print("Num Partitions before: " + str(flightTimeCsvDF.rdd.getNumPartitions()))

# phân vùng lại
partitionedDF = flightTimeCsvDF.repartition(5)
print("Num Partitions after: " + str(partitionedDF.rdd.getNumPartitions()))
partitionedDF.groupBy(spark_partition_id()).count().show()

# write 
partitionedDF.write \
    .format("json") \
    .mode("overwrite") \ # mode ghi đè
    .option("path", "drive/MyDrive/.../data") \
    .save()

# write
flightTimeCsvDF.write\
    .format("json") \
    .mode("overwrite") \
    .option("path", "drive/MyDrive/.../data") \
    .partitionBy('OP_CARRIER', 'ORIGIN') \  # write phân vùng theo 2 cột 
    .option("maxRecordsPerFile", 10000) \  # giới hạn số lượng record trên file
    .save()
