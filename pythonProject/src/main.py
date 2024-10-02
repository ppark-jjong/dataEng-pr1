import logging
from producer import KafkaProducer
from googleApi import get_sheet_data, preprocess_data

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def producer_task():
    producer = KafkaProducer()
    data_list = get_sheet_data()
    if data_list:
        dashboard_data, monthly_volume_data = preprocess_data(data_list)
        producer.send_to_kafka('dashboard_status', dashboard_data)
        producer.send_to_kafka('monthly_volume_status', monthly_volume_data)
    producer.flush()

if __name__ == "__main__":
    logger.info("Kafka 프로듀서 작업을 시작합니다.")
    producer_task()
