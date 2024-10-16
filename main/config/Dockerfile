# 베이스 이미지 설정
FROM python:3.10-slim

# 작업 디렉토리 설정
WORKDIR /app

# 필요한 시스템 패키지 설치 (Java, Curl 등)
RUN apt-get update && apt-get install -y \
    default-jdk \
    curl \
    libatlas-base-dev  # numpy 관련 호환성 문제 해결을 위한 패키지 추가

# Spark 다운로드 및 설치
RUN curl -o /tmp/spark-3.3.2-bin-hadoop3.tgz \
    https://archive.apache.org/dist/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz && \
    tar -xvf /tmp/spark-3.3.2-bin-hadoop3.tgz -C /usr/local && \
    ln -s /usr/local/spark-3.3.2-bin-hadoop3 /usr/local/spark && \
    rm /tmp/spark-3.3.2-bin-hadoop3.tgz

# 환경 변수 설정
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV SPARK_HOME=/usr/local/spark
ENV PYTHONPATH="${PYTHONPATH}:/app"
ENV PATH="$PATH:/usr/local/spark/bin"

# requirements.txt 파일 복사 및 Python 패키지 설치
COPY config/requirements.txt /app/config/requirements.txt
RUN pip install --no-cache-dir -r /app/config/requirements.txt

# 어플리케이션 파일 복사
COPY ../kafka .

# Docker 컨테이너에서 실행될 명령어 설정
CMD ["python", "/app/src/main.py"]
