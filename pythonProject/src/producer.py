from confluent_kafka import Producer
import logging
from config_manager import ConfigManager

config = ConfigManager()


class KafkaProducer:
    def __init__(self):
        self.producer = config.get_kafka_producer()  # 'localhost:9092' 대신 'kafka:9092' 사용

    def delivery_report(self, err, msg):
        if err is not None:
            logging.error(f"메시지 전송 실패: {err}")
        else:
            logging.info(f"메시지 전송 성공: {msg.topic()}")

    def send_to_kafka(self, topic, message):
        try:
            self.producer.produce(topic, value=message.SerializeToString(), callback=self.delivery_report)
            self.producer.poll(1)
            logging.info(f"{topic} 토픽으로 데이터 전송 성공")
        except Exception as e:
            logging.error(f"Kafka로 {topic} 데이터 전송 중 오류 발생: {e}")

    def flush(self):
        self.producer.flush()
