from confluent_kafka import Consumer, KafkaError
import logging
from config_manager import ConfigManager
from proto import realtime_status_pb2
from proto import weekly_analysis_pb2
from proto import monthly_analysis_pb2
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StringType, StructType, StructField

config = ConfigManager()

# 한글 로그 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

class KafkaConsumer:
    def __init__(self):
        self.consumer = Consumer({
            'bootstrap.servers': config.KAFKA_BOOTSTRAP_SERVERS,
            'group.id': 'my_consumer_group',
            'auto.offset.reset': 'earliest'
        })

    def consume_messages(self):
        self.consumer.subscribe([config.KAFKA_TOPICS['realtime_status'],
                                 config.KAFKA_TOPICS['weekly_analysis'],
                                 config.KAFKA_TOPICS['monthly_analysis']])
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error(f"Kafka 오류 발생: {msg.error()}")
                        continue

                topic = msg.topic()
                logger.info(f"{topic} 토픽에서 메시지를 수신함")

                if topic == config.KAFKA_TOPICS['realtime_status']:
                    proto_message = realtime_status_pb2.RealtimeStatus()
                elif topic == config.KAFKA_TOPICS['weekly_analysis']:
                    proto_message = weekly_analysis_pb2.WeeklyAnalysis()
                elif topic == config.KAFKA_TOPICS['monthly_analysis']:
                    proto_message = monthly_analysis_pb2.MonthlyAnalysis()

                proto_message.ParseFromString(msg.value())
                self.process_message(topic, proto_message)

        except Exception as e:
            logger.error(f"Kafka 컨슈머 오류 발생: {e}")
        finally:
            self.consumer.close()

    def process_message(self, topic, proto_message):
        if topic == config.KAFKA_TOPICS['realtime_status']:
            logger.info(
                f"실시간 배송 상태: 픽업 {proto_message.picked_count}건, 배송 {proto_message.shipped_count}건, POD {proto_message.pod_count}건")
            logger.info(f"완료율: {proto_message.completion_rate:.2f}%")
            logger.info(f"평균 배송 시간: {proto_message.avg_delivery_time}분")
        elif topic == config.KAFKA_TOPICS['weekly_analysis']:
            logger.info(f"이번 주 평균 배송 거리: {proto_message.avg_distance}km")
            logger.info(f"이슈 발생 횟수: {proto_message.issue_count}")
            logger.info(f"이슈 발생 DPS 목록: {proto_message.issues_dps}")
        elif topic == config.KAFKA_TOPICS['monthly_analysis']:
            logger.info(f"이번 달 완료율: {proto_message.weekly_completion_rate:.2f}%")
            logger.info(f"요일별 이슈 발생 패턴: {proto_message.issue_pattern}")
            logger.info(f"SLA 타입별, 요일별 배송 건수: {proto_message.sla_counts}")

# PySpark 데이터 처리 로직
def process_with_pyspark():
    spark = SparkSession.builder \
        .appName("Kafka PySpark Integration") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1") \
        .getOrCreate()

    logger.info("PySpark 세션 시작")

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", config.KAFKA_TOPICS['realtime_status']) \
        .load()

    schema = StructType([
        StructField("picked_count", StringType(), True),
        StructField("shipped_count", StringType(), True),
        StructField("pod_count", StringType(), True),
        StructField("completion_rate", StringType(), True),
        StructField("avg_delivery_time", StringType(), True)
    ])

    df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*") \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .start() \
        .awaitTermination()

    logger.info("PySpark 데이터 스트리밍 작업 완료")
