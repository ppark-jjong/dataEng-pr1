from confluent_kafka import Producer

# Kafka 브로커 설정
conf = {
    'bootstrap.servers': 'localhost:9092'  # Docker Compose에서 설정한 포트
}

# 프로듀서 생성
producer = Producer(**conf)

# 전달할 메시지
topic = 'test_topic'

# 메시지 리스트 (키-밸류 쌍)
messages = [
    ('key1', 'value1'),
    ('key2', 'value2'),
    ('key3', 'value3')
]

# 메시지 전달 콜백 함수
def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

# 키-밸류 쌍 메시지 전송
for key, value in messages:
    producer.produce(topic, key=key.encode('utf-8'), value=value.encode('utf-8'), callback=delivery_report)

# 메시지 전송이 완료될 때까지 대기
producer.flush()
