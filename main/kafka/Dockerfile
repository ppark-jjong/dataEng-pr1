# 베이스 이미지
FROM python:3.10-slim

# 필요한 패키지 설치
RUN apt-get update && apt-get install -y protobuf-compiler

# 작업 디렉토리 설정
WORKDIR /app

# Python 패키지 설치
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 소스 코드 복사
COPY . .

# 프로토콜 버퍼 컴파일
RUN protoc --python_out=. --proto_path=proto proto/realtime_status.proto
RUN protoc --python_out=. --proto_path=proto proto/weekly_analysis.proto
RUN protoc --python_out=. --proto_path=proto proto/monthly_analysis.proto

# 앱 실행
CMD ["python", "main.py"]
