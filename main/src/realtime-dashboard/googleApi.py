import pandas as pd
import datetime
import logging
from config_manager import ConfigManager

config = ConfigManager()

# 한글 로그 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


def get_sheet_data():
    service = config.get_sheets_service()
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=config.SHEET_ID, range=config.RANGE_NAME).execute()
    rows = result.get('values', [])

    if not rows:
        logger.error("Google Sheets에서 데이터를 찾을 수 없습니다.")
        return None

    logger.info(f"{len(rows)}개의 데이터를 Google Sheets에서 가져왔습니다.")
    return rows


def preprocess_data(data):
    df = pd.DataFrame(data, columns=[
        '배송 기사', 'Date(접수일)', 'DPS#', 'ETA', 'SLA', 'Ship to', 'Status',
        '1. Picked', '2. Shipped', '3. POD', 'Zip Code', 'Billed Distance (Put into system)',
        '인수자', 'issue'
    ])

    today = datetime.datetime.now().strftime('%Y-%m-%d')
    this_week_start = (datetime.datetime.now() - datetime.timedelta(days=datetime.datetime.now().weekday())).strftime(
        '%Y-%m-%d')
    this_month_start = datetime.datetime.now().replace(day=1).strftime('%Y-%m-%d')

    ### 1. 실시간 배송 상태 및 KPI 모니터링
    today_data = df[df['Date(접수일)'] == today]
    status_counts = today_data['Status'].value_counts().to_dict()
    total_orders = len(today_data)
    completed_orders = len(today_data[today_data['Status'] == 'Completed'])
    completion_rate = (completed_orders / total_orders) * 100 if total_orders > 0 else 0
    avg_delivery_time = today_data['Billed Distance (Put into system)'].astype(float).mean()

    realtime_data = {
        'picked_count': status_counts.get('Picked', 0),
        'shipped_count': status_counts.get('Shipped', 0),
        'pod_count': status_counts.get('POD', 0),
        'completion_rate': completion_rate,
        'avg_delivery_time': avg_delivery_time
    }

    ### 2. 이번 주 이슈 모니터링
    week_data = df[df['Date(접수일)'] >= this_week_start]
    issue_data = week_data[week_data['issue'] == 'O']
    issues_dps = issue_data['DPS#'].tolist()
    avg_distance = week_data['Billed Distance (Put into system)'].astype(float).mean()

    weekly_data = {
        'avg_distance': avg_distance,
        'issues_dps': issues_dps,
        'issue_count': len(issues_dps)
    }

    ### 3. 이번 달 배송 완료율 및 트렌드 분석
    month_data = df[df['Date(접수일)'] >= this_month_start]
    weekly_completion_rate = (len(month_data[month_data['Status'] == 'Completed']) / len(month_data)) * 100 if len(
        month_data) > 0 else 0

    month_data['요일'] = pd.to_datetime(month_data['Date(접수일)']).dt.day_name()
    issue_pattern = month_data[month_data['issue'] == 'O'].groupby('요일').size().to_dict()
    sla_counts = month_data.groupby(['SLA', '요일']).size().unstack(fill_value=0).to_dict()

    monthly_data = {
        'weekly_completion_rate': weekly_completion_rate,
        'issue_pattern': issue_pattern,
        'sla_counts': sla_counts
    }

    return realtime_data, weekly_data, monthly_data
