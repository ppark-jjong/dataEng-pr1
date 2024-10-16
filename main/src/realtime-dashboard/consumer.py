from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StringType, StructType, StructField
import logging
from config_manager import ConfigManager

config = ConfigManager()

# 로그 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

def process_with_pyspark():
    spark = SparkSession.builder \
        .appName("Kafka PySpark Integration") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1") \
        .getOrCreate()

    logger.info("PySpark 세션 시작")

    # Kafka에서 데이터 읽기
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", config.KAFKA_TOPICS['realtime_status']) \
        .load()

    # 데이터 스키마 정의
    schema = StructType([
        StructField("picked_count", StringType(), True),
        StructField("shipped_count", StringType(), True),
        StructField("pod_count", StringType(), True),
        StructField("completion_rate", StringType(), True),
        StructField("avg_delivery_time", StringType(), True)
    ])

    # JSON 변환 및 데이터 처리
    transformed_df = df.selectExpr("CAST(value AS STRING)") \
                       .select(from_json(col("value"), schema).alias("data")) \
                       .select("data.*")

    # 변환된 데이터 로그 출력 및 스트리밍 시작
    query = transformed_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()  # 스트리밍을 시작함

    logger.info("Kafka 스트리밍 데이터를 처리 중...")

    # 스트리밍 작업 종료 대기 (스트리밍이 종료되지 않도록)
    query.awaitTermination()
