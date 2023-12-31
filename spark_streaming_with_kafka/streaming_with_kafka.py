from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, IntegerType, ArrayType

spark = SparkSession.builder\
                    .appName('myapp')\
                    .master('local')\
                    .config("spark.streaming.stopGracefullyOnShutdown", "true")\
                    .getOrCreate()

#Tạo schema cho dữ liệu đầu vào: đây là cấu trúc của dữ liệu 
schema = StructType([
	StructField("InvoiceNumber", StringType()),
	StructField("CreatedTime", LongType()),
	StructField("StoreID", StringType()),
	StructField("PosID", StringType()),
	StructField("CashierID", StringType()),
	StructField("CustomerType", StringType()),
	StructField("CustomerCardNo", StringType()),
	StructField("TotalAmount", DoubleType()),
	StructField("NumberOfItems", IntegerType()),
	StructField("PaymentMethod", StringType()),
	StructField("CGST", DoubleType()),
	StructField("SGST", DoubleType()),
	StructField("CESS", DoubleType()),
	StructField("DeliveryType", StringType()),
	StructField("DeliveryAddress", StructType([
		StructField("AddressLine", StringType()),
		StructField("City", StringType()),
		StructField("State", StringType()),
		StructField("PinCode", StringType()),
		StructField("ContactNumber", StringType())
	])),
	StructField("InvoiceLineItems", ArrayType(StructType([
		StructField("ItemCode", StringType()),
		StructField("ItemDescription", StringType()),
		StructField("ItemPrice", DoubleType()),
		StructField("ItemQty", IntegerType()),
		StructField("TotalValue", DoubleType())
	]))),
])

# Đọc dữ liệu từ Kafka
kafka_df = spark.readStream\
                .format('kafka')\
                .option('kafka.bootstrap.servers', 'localhost:9092')\
                .option('subscribe', 'invoices')\
                .option('startingOffsets', 'earliest')\
                .load()

# Chuyển dữ liệu từ dạng JSON về MapType(): do dữ liệu trong kafka có dạng key-value nên phải chuyển về dạng DF
value_df = kafka_df.select(from_json(col('value').cast('string'), schema).alias("value"))
#Xử lý dữ liệu
notification_df = value_df.select("value.InvoiceNumber", "value.CustomerCardNo", "value.TotalAmount") \
	.withColumn("EarnedLoyaltyPoints", expr("TotalAmount * 0.2"))

# Ghi dữ liệu ở dạng Kafka Sink
# Chuyển đổi Dataframe về dạng key - value: chuyển về dạng key-value sẵn sàng write vào kafka
kafka_target_df = notification_df.selectExpr('InvoiceNumber as key', 
                                             """
						to_json(named_struct(
                                            'CustomerCardNo', CustomerCardNo,
                                            'TotalAmount', TotalAmount,
                                            'EarnedLoyaltyPoints', TotalAmount * 0.2
						)) as value
						 """
                                             )
notification_writer_query = kafka_target_df.writeStream\
	.queryName('Notification Writer')\
    .format('kafka')\
    .option('kafka.bootstrap.servers', 'localhost:9092')\
    .option('topic', 'notifications')\
    .outputMode('append')\
    .option('checkpointLocation', 'chk-point-dir/notify')\  # do đang write ra 2 data sink trong cùng ứng dụng nên vậy
    .start()

# Trích xuất các dữ liệu
explode_df = value_df.selectExpr("value.InvoiceNumber", "value.CreatedTime", "value.StoreID",
									"value.PosID", "value.CustomerType", "value.PaymentMethod", "value.DeliveryType",
									"value.DeliveryAddress.City",
									"value.DeliveryAddress.State", "value.DeliveryAddress.PinCode",
									"explode(value.InvoiceLineItems) as LineItem")

flattened_df = explode_df \
	.withColumn("ItemCode", expr("LineItem.ItemCode")) \
	.withColumn("ItemDescription", expr("LineItem.ItemDescription")) \
	.withColumn("ItemPrice", expr("LineItem.ItemPrice")) \
	.withColumn("ItemQty", expr("LineItem.ItemQty")) \
	.withColumn("TotalValue", expr("LineItem.TotalValue")) \
	.drop("LineItem")
# Ghi dữ liệu dưới dạng File Sink
invoice_writer_query = flattened_df.writeStream \
	.format('json') \
	.queryName("Flattened Invoice Writer") \
	.outputMode('append') \
	.option("path", "output") \
	.option("checkpointLocation", 'chk-point-dir/flatten') \
	.start()

print("Waiting for Queries")
spark.streams.awaitAnyTermination()

