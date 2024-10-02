from confluent_kafka import Producer
import logging
from config_manager import ConfigManager
from proto import realtime_status_pb2  # 실시간 상태
from proto import weekly_analysis_pb2  # 이번 주 분석
from proto import monthly_analysis_pb2  # 이번 달 분석

config = ConfigManager()

class KafkaProducer:
    def __init__(self):
        self.producer = config.get_kafka_producer()

    def delivery_report(self, err, msg):
        if err is not None:
            logging.error(f"메시지 전송 실패: {err}")
        else:
            logging.info(f"메시지 전송 성공: {msg.topic()}")

    def send_to_kafka(self, topic, message):
        try:
            if topic == 'realtime_status':
                proto_message = realtime_status_pb2.RealtimeStatus(
                    picked_count=message['picked_count'],
                    shipped_count=message['shipped_count'],
                    pod_count=message['pod_count'],
                    completion_rate=message['completion_rate'],
                    avg_delivery_time=message['avg_delivery_time']
                )
            elif topic == 'weekly_analysis':
                proto_message = weekly_analysis_pb2.WeeklyAnalysis(
                    avg_distance=message['avg_distance'],
                    issues_dps=message['issues_dps'],
                    issue_count=message['issue_count']
                )
            elif topic == 'monthly_analysis':
                proto_message = monthly_analysis_pb2.MonthlyAnalysis(
                    weekly_completion_rate=message['weekly_completion_rate'],
                    issue_pattern=message['issue_pattern'],
                    sla_counts=message['sla_counts']
                )

            # 직렬화 후 Kafka로 전송
            self.producer.produce(topic, value=proto_message.SerializeToString(), callback=self.delivery_report)
            self.producer.poll(1)
            logging.info(f"{topic} 토픽으로 데이터 전송 성공")
        except Exception as e:
            logging.error(f"Kafka로 {topic} 데이터 전송 중 오류 발생: {e}")

    def flush(self):
        self.producer.flush()
