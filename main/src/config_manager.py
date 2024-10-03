import os
from google.oauth2 import service_account
from googleapiclient.discovery import build
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient

class ConfigManager:
    def __init__(self):
        # Google Sheets API 설정
        self.SHEET_ID = '1x4P2VO-ZArT7ibSYywFIBXUTapBhUnE4_ouVMKrKBwc'
        self.RANGE_NAME = 'Sheet1!A2:n637'
        self.SERVICE_ACCOUNT_FILE = '/app/oauth/google/credentials.json'
        # self.SERVICE_ACCOUNT_FILE = 'C:/MyMain/dataEng/main/oauth/google/credentials.json'

        # Kafka 설정
        self.KAFKA_BOOTSTRAP_SERVERS = 'kafka:9092'
        self.KAFKA_TOPICS = {
            'dashboard_status': 'dashboard_status',
            'monthly_volume_status': 'monthly_volume_status'
        }

        # Excel 저장 경로
        self.EXCEL_SAVE_PATH = "/app/xlsx"

    def get_sheets_service(self):
        credentials = service_account.Credentials.from_service_account_file(
            self.SERVICE_ACCOUNT_FILE, scopes=['https://www.googleapis.com/auth/spreadsheets.readonly'])
        return build('sheets', 'v4', credentials=credentials)

    def get_kafka_producer(self):
        return Producer({'bootstrap.servers': self.KAFKA_BOOTSTRAP_SERVERS})

    def get_kafka_admin_client(self):
        return AdminClient({'bootstrap.servers': self.KAFKA_BOOTSTRAP_SERVERS})

    def get_excel_save_path(self, filename):
        if not os.path.exists(self.EXCEL_SAVE_PATH):
            os.makedirs(self.EXCEL_SAVE_PATH)
        return os.path.join(self.EXCEL_SAVE_PATH, filename)
