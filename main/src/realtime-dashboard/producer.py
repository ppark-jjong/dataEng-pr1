from confluent_kafka import Producer, KafkaError
import logging
from confluent_kafka.admin import AdminClient, NewTopic
import proto.realtime_status_pb2 as realtime_status_pb2
import proto.weekly_analysis_pb2 as weekly_analysis_pb2
import proto.monthly_analysis_pb2 as monthly_analysis_pb2
from config_manager import ConfigManager

config = ConfigManager()

class KafkaProducer:
    def __init__(self):
        self.producer = config.get_kafka_producer()
        self.admin_client = AdminClient({'bootstrap.servers': config.KAFKA_BOOTSTRAP_SERVERS})
        self.create_topics_if_not_exists(['realtime_status', 'weekly_analysis', 'monthly_analysis'])

    def create_topics_if_not_exists(self, topics):
        """ 토픽이 없으면 자동으로 생성 """
        topic_metadata = self.admin_client.list_topics(timeout=10)
        existing_topics = topic_metadata.topics.keys()

        for topic in topics:
            if topic not in existing_topics:
                new_topic = NewTopic(topic, num_partitions=1, replication_factor=1)
                self.admin_client.create_topics([new_topic])
                logging.info(f"토픽 '{topic}' 생성 완료")

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

            # Protobuf 직렬화 후 Kafka로 전송
            serialized_message = proto_message.SerializeToString()
            self.producer.produce(topic, value=serialized_message, callback=self.delivery_report)
            self.producer.poll(1)
            logging.info(f"{topic} 토픽으로 데이터 전송 성공")
        except Exception as e:
            logging.error(f"Kafka로 {topic} 데이터 전송 중 오류 발생: {e}")

    def flush(self):
        self.producer.flush()
