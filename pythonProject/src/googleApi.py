import pandas as pd
from config_manager import ConfigManager

config = ConfigManager()

def get_sheet_data():
    service = config.get_sheets_service()

    # Google Sheets에서 데이터 가져오기
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=config.SHEET_ID, range=config.RANGE_NAME).execute()
    rows = result.get('values', [])

    if not rows:
        print("Google Sheets에서 데이터를 찾을 수 없습니다.")
        return None

    return rows

def preprocess_data(data):
    # 데이터 전처리 로직
    df = pd.DataFrame(data, columns=['Column1', 'Column2', 'Column3'])
    # 필요한 데이터 처리 후 직렬화된 형태로 반환
    return df.to_dict(), df.to_dict()
