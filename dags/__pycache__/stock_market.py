from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook #API를 가져오기위한 메소드클래스?
from airflow.sensors.base import PokeReturnValue
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime
import requests

from include.stock_market.tasks import _get_stock_prices, _store_prices, _get_formatted_csv

SYMBOL = 'AAPL'

@dag(
    start_date=datetime(2023, 1, 1),    #시작 날짜
    schedule='@daily',                  #trigger 빈도 = 매일 자정
    catchup=False,                      #비 트리거 상태의 Dag 런은 실행 X
    tags=['stock_market']               #Airflow UI의 데이터 파이프라인을 필터링하는덷 유용해서 함
)
def stock_market():                     #데이터 파이프라인의 Dag ID: 데파의 고유 식별자

    @task.sensor(poke_interval=30, timeout=300, mode='poke')
    def is_api_available() -> PokeReturnValue:
        api = BaseHook.get_connection('stock_api')
        url = f"{api.host}{api.extra_dejson['endpoint']}"   #이 url이 사용가능한지 테스트하는 것
        response = requests.get(url, headers=api.extra_dejson['headers'])
        condition = response.json()['finance']['result'] is None
        return PokeReturnValue(is_done=condition, xcom_value=url)

    get_stock_prices = PythonOperator(
        task_id='get_stock_prices',
        python_callable=_get_stock_prices,      #_get_stock_prices라는 Python 함수를 호출할 것을 지정합니다. 이 함수는 태스크가 실행될 때 호출됩니다.
        op_kwargs={'url': '{{ task_instance.xcom_pull(task_ids="is_api_available") }}', 'symbol': SYMBOL}        #_get_stock_prices 함수에 전달할 키워드 인수들을 정의
    )

    store_prices = PythonOperator(
        task_id="store_prices",
        python_callable=_store_prices,
        op_kwargs={'stock': '{{ task_instance.xcom_pull(task_ids="get_stock_prices") }}'}
    )

    format_prices = DockerOperator(
        task_id='format_prices',
        image='airflow/stock-app',
        container_name='format_prices',
        api_version='auto',
        auto_remove=True,
        docker_url='tcp://docker-proxy:2375',
        network_mode='container:spark-master',
        tty=True,
        xcom_all=False,
        mount_tmp_dir=False,
        environment={
            'SPARK_APPLICATION_ARGS': '{{ task_instance.xcom_pull(task_ids="store_prices") }}'
        }
    )

    get_formatted_csv = PythonOperator(
        task_id="get_formatted_csv",
        python_callable=_get_formatted_csv,
        op_kwargs={
            'path': '{{ task_instance.xcom_pull(task_ids="store_prices") }}'
        }
    )

    is_api_available() >> get_stock_prices >> store_prices >> format_prices >> get_formatted_csv   #종속성 정의
stock_market()
#이러고 Airflow UI 확인