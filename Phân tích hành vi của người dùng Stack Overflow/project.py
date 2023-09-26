import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re
from pyspark.sql import Window

# config để connect tới mongo
spark = SparkSession\
    .builder\
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:10.2.0')\
    .master('local')\
    .appName('myapp')\
    .config('spark.executor.memory', '1g')\
    .config('spark.mongodb.read.connection.uri', 'mongodb://127.0.0.1/dep303_ass1')\
    .getOrCreate()

# Đọc và chuẩn hóa dữ liệu
dfQ = spark.read \
    .format('mongodb') \
    .option("spark.mongodb.read.connection.uri", "mongodb://127.0.0.1/dep303_ass1.Questions")\
    .option('inferSchema', 'true') \
    .load()
# dfQ.printSchema()
dfQ = dfQ.withColumn('OwnerUserId', when(col('OwnerUserId') == 'NA', None).otherwise(col('OwnerUserId')))\
    .drop('_id')
dfQ = dfQ.withColumn('OwnerUserId', col('OwnerUserId').cast(IntegerType()))\
    .withColumn('CreationDate', col('CreationDate').cast(DateType()))\
    .withColumn('ClosedDate', col('ClosedDate').cast(DateType()))
dfQ.show()

# Yêu cầu 1: Tính số lần xuất hiện của các ngôn ngữ lập trình
def extract_body(body):
    lang_regex = r"Java|Python|C\+\+|C\#|Go|Ruby|Javascript|PHP|HTML|CSS|SQL"
    if body is not None:
        return re.findall(lang_regex, body)
extract_language_udf = udf(extract_body, returnType=ArrayType(StringType()))
language_df = dfQ.withColumn('body_extract', extract_language_udf(col('Body')))\
    .withColumn('Programing Language', explode('body_extract'))\
    .groupBy('Programing Language')\
    .agg(count('Programing Language').alias('Count'))
language_df.show()

# Yêu cầu 2 : Tính tổng điểm của User theo từng ngày
dfQ.createOrReplaceTempView('questions')
user_score = spark.sql('''
    select
        OwnerUserId,
        CreationDate, 
        sum(Score) as TotalScoreInDay
    from questions
    group by OwnerUserId, CreationDate
    order by OwnerUserId, CreationDate
''')
running_total_window = Window.partitionBy('OwnerUserId')\
                             .orderBy('CreationDate')\
                             .rowsBetween(Window.unboundedPreceding, Window.currentRow)
user_score = user_score.withColumn('TotalScore', sum('TotalScoreInDay').over(running_total_window))
# user_score.filter(col('OwnerUserId') != null)
user_score = user_score.filter(col('OwnerUserId').isNotNull())
user_score.drop('TotalScoreInDay').orderBy(['OwnerUserId', 'CreationDate']).show()

# Yêu cầu 3: Tính tổng số điểm mà User đạt được trong một khoảng thời gian

START = '2008-01-01'
END = '2009-01-01'
score_df = spark.sql('''
    select
        OwnerUserId,
        sum(Score) as TotalScore
    from questions
    where CreationDate between cast('{0}' as date) and cast('{1}' as date) 
    group by OwnerUserId
'''.format(START, END))
score_df.orderBy(col('OwnerUserId')).show()

# Yêu cầu 4: Tìm các câu hỏi có nhiều câu trả lời
# Do thao tác có thể tốn rất nhiều thời gian, nên ta sử dụng cơ chế Bucket Join để phân vùng cho các dữ liệu trước
spark.sql('create database if not exists mydb') # tạo db trong spark
spark.catalog.setCurrentDatabase('mydb')
# đọc file Answers
dfA = spark.read \
    .format('mongodb') \
    .option("spark.mongodb.read.connection.uri", "mongodb://127.0.0.1/dep303_ass1.Answers")\
    .option('inferSchema', 'true') \
    .load()
# dfQ.printSchema()
dfA = dfA.withColumn('OwnerUserId', when(col('OwnerUserId') == 'NA', None).otherwise(col('OwnerUserId')))\
    .drop('_id')
dfA = dfA.withColumn('OwnerUserId', col('OwnerUserId').cast(IntegerType()))\
    .withColumn('CreationDate', col('CreationDate').cast(DateType()))
dfA.show()

spark.conf.set('spark.sql.shuffle.partitions', 3) # set shuffles partition bằng 3
# shuffles trước join bằng bucket
dfQ.coalesce(1).write\
    .bucketBy(3, 'Id')\
    .mode('overwrite')\
    .saveAsTable('mydb.Questions')

dfA.coalesce(1).write\
    .bucketBy(3, 'ParentId')\
    .mode('overwrite')\
    .saveAsTable('mydb.Answers')
# đọc table vừa được lưu ở trên
Tb_Q = spark.read.table('mydb.Questions')
Tb_A = spark.read.table('mydb.Answers')
# join 
join_expr = Tb_Q.Id == Tb_A.ParentId
join_df = Tb_Q.join(Tb_A, join_expr, 'inner')
# Tìm các câu hỏi có nhiều hơn 5 câu trả lời
join_df = join_df.select(Tb_A.Id, 'ParentId')
join_df.groupBy(col('ParentId')).agg(count('Id').alias('count')).filter(col('count') > 5).orderBy('ParentId').show()
# join_df.show()
