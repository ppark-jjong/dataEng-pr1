from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StringType, StructType, StructField

# Spark 세션 생성
spark = SparkSession.builder \
    .appName("Kafka-PySpark-Consumer") \
    .getOrCreate()

# Kafka에서 읽어들일 메시지 구조 정의
schema = StructType([
    StructField("id", StringType(), True),
    StructField("message", StringType(), True)
])

# Kafka 스트리밍 설정
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test_topic") \
    .load()

# 메시지 역직렬화
parsed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# 콘솔로 출력 (한글 메시지 출력 확인)
query = parsed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
