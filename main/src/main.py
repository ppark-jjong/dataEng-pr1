from producer import KafkaProducer
from googleApi import get_sheet_data, preprocess_data
import logging

# 한글 로그 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


def producer_task():
    # Google Sheets에서 데이터 가져오기
    data_list = get_sheet_data()
    if not data_list:
        logger.error("Google Sheets 데이터가 없습니다.")
        return

    # 데이터 전처리
    realtime_data, weekly_data, monthly_data = preprocess_data(data_list)

    # Kafka로 데이터 전송
    producer = KafkaProducer()
    producer.send_to_kafka('realtime_status', realtime_data)
    producer.send_to_kafka('weekly_analysis', weekly_data)
    producer.send_to_kafka('monthly_analysis', monthly_data)
    producer.flush()


if __name__ == '__main__':
    producer_task()
