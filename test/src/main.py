import threading
from producer import send_message
from consumer import *

# Producer 실행 함수
def producer_thread():
    topic_name = "test_topic"
    message = {"id": 2, "message": "PySpark가 Kafka에서 데이터를 읽고 있습니다."}
    print(f"'{topic_name}' 토픽으로 메시지 전송을 시작합니다.")
    send_message(topic_name, message)
    print(f"'{topic_name}' 토픽으로 메시지 전송을 완료했습니다.")

# PySpark Consumer 실행 함수
def consumer_thread():
    print("PySpark가 Kafka에서 메시지를 소비하는 중입니다...")
    start_spark_consumer()  # 위 consumer.py에 작성한 코드를 실행

if __name__ == "__main__":
    # Producer와 Consumer를 각각의 스레드로 실행
    t1 = threading.Thread(target=producer_thread)
    t2 = threading.Thread(target=consumer_thread)

    t1.start()
    t2.start()

    t1.join()
    t2.join()
