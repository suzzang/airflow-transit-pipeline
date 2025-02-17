import requests
import pandas as pd
import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow.models import Variable
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

SEOUL_API_KEY = Variable.get("SEOUL_API_KEY")
#print(SEOUL_API_KEY)

POSTGRES_CONFIG = {
    "host": Variable.get("POSTGRES_HOST"),
    "database": Variable.get("POSTGRES_DB"),
    "user": Variable.get("POSTGRES_USER"),
    "password": Variable.get("POSTGRES_PASSWORD"),
    "port": Variable.get("POSTGRES_PORT"),
}

DB_URL = f"postgresql+psycopg2://{POSTGRES_CONFIG['user']}:{POSTGRES_CONFIG['password']}@{POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['database']}"
engine = create_engine(DB_URL)

def fetch_subway_data(**kwargs):
    """
    서울시 지하철 승하차 인원 데이터를 API에서 가져오는 함수.
    매일 실행되며, 자동으로 3일 전 날짜를 계산하여 API 요청을 보냄.
    """
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
        data = json.loads(response.text)  # 명시적으로 JSON 변환
    except json.JSONDecodeError:
        raise Exception(f"JSON 디코딩 실패! 응답 내용: {response.text}")

    # API 응답 데이터가 올바른지 확인
    if "CardSubwayStatsNew" not in data or "row" not in data["CardSubwayStatsNew"]:
        raise Exception(f"API 응답 구조 오류! 응답 내용: {data}")

    # pandas json_normalize() 활용
    df = pd.json_normalize(data["CardSubwayStatsNew"]["row"])

    # 컬럼명 변경 (API 응답 필드 확인 필요)
    df = df.rename(columns={
        "USE_YMD": "date",
        "SBWY_ROUT_LN_NM": "line",
        "SBWY_STNS_NM": "station",
        "GTON_TNOPE": "passenger_in",
        "GTOFF_TNOPE": "passenger_out",
        "REG_YMD": "register_date",
    })
    
    # 정수형으로 변환
    df["passenger_in"] = df["passenger_in"].astype(int)
    df["passenger_out"] = df["passenger_out"].astype(int)

    # CSV 저장
    # filename = f"./data/subway_data_{target_date}.csv"
    # df.to_csv(filename, index=False)
    # print(f"지하철 데이터 저장 완료: {filename}")
    
    # 데이터 반환
    return df.to_dict(orient="records")

def load_data_to_postgres(**kwargs):
    """
    데이터를 PostgreSQL에 적재하는 함수
    """
    ti = kwargs["ti"]
    subway_data = ti.xcom_pull(task_ids="fetch_subway_data")  # 앞 단계에서 데이터 가져오기

    if not subway_data:
        raise Exception("가져온 데이터가 없습니다!")

    df = pd.DataFrame(subway_data)

    print(f"PostgreSQL에 저장할 데이터 미리보기:\n{df.head()}")

    # 테이블 생성 쿼리
    create_table_query = """
    CREATE TABLE IF NOT EXISTS subway_passengers (
        id SERIAL PRIMARY KEY,
        date VARCHAR(8),
        line VARCHAR(10),
        station VARCHAR(50),
        passenger_in INTEGER,
        passenger_out INTEGER,
        register_date date VARCHAR(8)
    );
    """
    with engine.connect() as conn:
        conn.execute(create_table_query)

    # 데이터 삽입
    try:
        with engine.connect() as conn:
            df.to_sql("subway_passengers", conn, if_exists="append", index=False)
        print(f"{len(df)}개의 데이터가 PostgreSQL에 저장되었습니다.")
    except SQLAlchemyError as e:
        print(f"PostgreSQL 저장 중 오류 발생: {str(e)}")
        raise

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

load_data_task = PythonOperator(
    task_id="load_data_to_postgres_task",
    python_callable=load_data_to_postgres,
    provide_context=True,
    dag=dag,
)

fetch_data_task >> load_data_task
