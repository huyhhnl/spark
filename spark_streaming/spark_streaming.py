import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

spark = SparkSession \
    .builder \
    .appName("Streaming Demo") \
    .master("local[3]") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.sql.streaming.schemaInference", "true") \
    .getOrCreate()

sc = spark.sparkContext
# đọc file gh 1 file mỗi batch
raw_df = spark.readStream\
              .format('json')\
              .option('path','input')\
              .option('maxFilesPerTrigger', '1')\
              .load()

# Làm phẳng các phần tử của mảng InvoiceLineItems, bạn có thể sử dụng explode.
explode_df = raw_df.selectExpr('explode(InvoiceLineItems) as LineItem')
# Lấy các dữ liệu từ Dataframe
flattened_df = explode_df \
	.withColumn("ItemCode", expr('LineItem.ItemCode')) \
	.withColumn("ItemDescription", expr('LineItem.ItemDescription')) \
	.withColumn("ItemPrice",expr('LineItem.ItemPrice')) \
	.withColumn("ItemQty", expr('LineItem.ItemQty')) \
	.withColumn("TotalValue", expr('LineItem.TotalValue')) \
	.drop("LineItem")

# Lưu dữ liệu và tạo các checkpoint
# Bạn hãy thiết lập để mỗi lần trigger cách nhau 1 phút và Output mode là Append
invoiceWriterQuery = flattened_df.writeStream \
	.format("json") \
	.queryName("Flattened Invoice Writer") \
	.outputMode('append') \
	.option("path", "output") \
	.option("checkpointLocation", "chk-point-dir") \
	.trigger(processingTime='1 minute')\
	.start()

print("Flattened Invoice Writer started")
invoiceWriterQuery.awaitTermination()


              
