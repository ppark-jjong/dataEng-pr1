from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, MapType
import logging
from config_manager import ConfigManager

config = ConfigManager()

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Kafka에서 데이터를 읽어오는 스키마 정의
dashboard_schema = StructType([
    StructField("picked_count", IntegerType(), True),
    StructField("shipped_count", IntegerType(), True),
    StructField("pod_count", IntegerType(), True),
    StructField("sla_counts_today", MapType(StringType(), IntegerType()), True),
    StructField("issues_today", StringType(), True)
])

monthly_volume_schema = StructType([
    StructField("sla_counts_month", MapType(StringType(), IntegerType()), True),
    StructField("weekday_counts", MapType(StringType(), IntegerType()), True),
    StructField("distance_counts", MapType(IntegerType(), IntegerType()), True)
])

def process_batch(df, epoch_id, schema, file_name):
    try:
        df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
        pandas_df = df.toPandas()
        save_to_excel(pandas_df, file_name)
    except Exception as e:
        logger.error(f"배치 처리 중 오류 발생: {e}")

def save_to_excel(pandas_df, file_name):
    file_path = config.get_excel_save_path(file_name)
    pandas_df.to_excel(file_path, index=False)
    logger.info(f"엑셀 파일로 저장 완료: {file_path}")

def start_spark_consumer():
    spark = config.get_spark_session()

    df_dashboard = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", config.KAFKA_TOPICS['dashboard_status']) \
        .option("startingOffsets", "earliest") \
        .load()

    query_dashboard = df_dashboard.writeStream \
        .foreachBatch(lambda df, epoch_id: process_batch(df, epoch_id, dashboard_schema, "dashboard_status.xlsx")) \
        .start()

    df_monthly_volume = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", config.KAFKA_TOPICS['monthly_volume_status']) \
        .option("startingOffsets", "earliest") \
        .load()

    query_monthly_volume = df_monthly_volume.writeStream \
        .foreachBatch(lambda df, epoch_id: process_batch(df, epoch_id, monthly_volume_schema, "monthly_volume_status.xlsx")) \
        .start()

    spark.streams.awaitAnyTermination()
