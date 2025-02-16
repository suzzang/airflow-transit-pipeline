import requests
import pandas as pd
import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow.models import Variable

SEOUL_API_KEY = Variable.get("SEOUL_API_KEY")
#print(SEOUL_API_KEY)

def fetch_subway_data(**kwargs):
    """
    서울시 지하철 승하차 인원 데이터를 API에서 가져와 CSV로 저장하는 함수.
    매일 실행되며, 자동으로 3일 전 날짜를 계산하여 API 요청을 보냄.
    """
    try:
        # 현재 실행 날짜에서 3일 전 날짜 계산 (YYYYMMDD 형식)
        target_date = (kwargs["execution_date"] - timedelta(days=3)).strftime("%Y%m%d")
        print(f"API 호출 날짜: {target_date}")

        # API 요청 URL
        url = f"http://openapi.seoul.go.kr:8088/{SEOUL_API_KEY}/json/CardSubwayStatsNew/1/1000/{target_date}/"

        # API 요청 보내기
        print(url)
        response = requests.get(url)

        # 응답 상태 코드 확인
        if response.status_code != 200:
            raise Exception(f" API 요청 실패! 상태 코드: {response.status_code}, 응답 내용: {response.text}")

        # JSON 변환 (예외 처리 추가)
        try:
            data = json.loads(response.text)  # ✅ 명시적으로 JSON 변환
        except json.JSONDecodeError:
            raise Exception(f"❌ JSON 디코딩 실패! 응답 내용: {response.text}")

        # API 응답 데이터가 올바른지 확인
        if "CardSubwayStatsNew" not in data or "row" not in data["CardSubwayStatsNew"]:
            raise Exception(f"❌ API 응답 구조 오류! 응답 내용: {data}")

        # pandas json_normalize() 활용
        df = pd.json_normalize(data["CardSubwayStatsNew"]["row"])

        # 컬럼명 변경 (API 응답 필드 확인 필요)
        df = df.rename(columns={
            "USE_YMD": "date",
            "LINE_NUM": "line",
            "SUB_STA_NM": "station",
            "RIDE_PASGR_NUM": "passenger_in",
            "ALIGHT_PASGR_NUM": "passenger_out",
        })

        # CSV 저장
        filename = f"./data/subway_data_{target_date}.csv"
        df.to_csv(filename, index=False)
        print(f"지하철 데이터 저장 완료: {filename}")

    except Exception as e:
        print(f"데이터 수집 중 오류 발생: {e}")
        raise e  # Airflow에서 오류 감지 가능하도록 예외 다시 발생

# Airflow DAG 설정
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 2, 16),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "subway_data_pipeline",
    default_args=default_args,
    description="서울 지하철 승하차 인원 데이터 수집 DAG",
    schedule_interval="30 19 * * *",  # 매일 오후 7:30 실행
)

fetch_data_task = PythonOperator(
    task_id="fetch_subway_data",
    python_callable=fetch_subway_data,
    provide_context=True,  # execution_date 사용
    dag=dag,
)

fetch_data_task
