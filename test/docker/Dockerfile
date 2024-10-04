
FROM openjdk:8-jdk

# 로컬 Spark 디렉토리에서 컨테이너로 복사
COPY /MyMain/interpreter/spark-3.3.2-bin-hadoop3 /opt/spark

# 환경 변수 설정
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:/opt/spark/bin:/opt/spark/sbin

# Python 설치 및 의존성 설치
RUN apt-get update && apt-get install -y python3 python3-pip
RUN pip3 install pyspark confluent-kafka protobuf

# 작업 디렉토리 설정
WORKDIR /app/src
COPY src/ /app/src

CMD ["python3", "main.py"]
