from confluent_kafka import Producer
import json

# Kafka Producer 설정
producer_config = {
    'bootstrap.servers': 'localhost:9092'
}

producer = Producer(producer_config)


# 메시지를 보내는 함수
def send_message(topic, message):
    try:
        producer.produce(topic, value=json.dumps(message))
        producer.flush()
        print(f"메시지 전송 완료: {message}")
    except Exception as e:
        print(f"메시지 전송 실패: {e}")


if __name__ == "__main__":
    topic_name = "test_topic"
    message = {"id": 1, "message": "Kafka와 PySpark 연동 테스트!"}

    print(f"'{topic_name}' 토픽으로 메시지 전송을 시작합니다.")
    send_message(topic_name, message)
    print(f"'{topic_name}' 토픽으로 메시지 전송을 완료했습니다.")
