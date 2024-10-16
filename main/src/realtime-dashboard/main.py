from producer import KafkaProducer
from consumer import process_with_pyspark
import logging

# 한글 로그 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

def producer_task():
    logger.info("프로듀서 작업을 시작합니다.")
    producer = KafkaProducer()

    producer.flush()
    logger.info("Kafka로 모든 데이터 전송 완료")

def consumer_task():
    logger.info("PySpark 컨슈머 작업을 시작합니다.")
    process_with_pyspark()

if __name__ == '__main__':
    producer_task()
    consumer_task()
