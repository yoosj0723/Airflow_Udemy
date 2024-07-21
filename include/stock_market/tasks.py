from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowNotFoundException
import requests
import json
from minio import Minio
from io import BytesIO

BUCKET_NAME = 'stock-market'

def _get_minio_client():
    minio=BaseHook.get_connection('minio')#.extra_dejson
    print(minio)
    client = Minio(         #client를 통해 minio와 연결
        endpoint=minio.extra_dejson['endpoint_url'].split('//')[1],
        access_key=minio.login,
        secret_key=minio.password,
        secure=False
    )
    return client

def _get_stock_prices(url, symbol):
    url = f"{url}{symbol}?metrics=high?&interval=1d&range=1y"
    api = BaseHook.get_connection('stock_api')
    response = requests.get(url, headers=api.extra_dejson['headers'])
    return json.dumps(response.json()['chart']['result'][0])

def _store_prices(stock):
    client = _get_minio_client()
    
    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)
    stock = json.loads(stock)               #a문자열을 Python dictionary로 변환
    symbol = stock['meta']['symbol']        #stock은 XComs로 갖고 와야 하므로 parmeter설정해야됨
    data = json.dumps(stock, ensure_ascii=False).encode('utf8')
    objw = client.put_object(
        bucket_name=BUCKET_NAME,
        object_name=f'{symbol}/prices.json',     #bucket에 symbol폴더 생성 후 prices.json파일 생성
        data=BytesIO(data),
        length=len(data)
    )
    return f'{objw.bucket_name}/{symbol}'

def _get_formatted_csv(path):
    client = _get_minio_client()
    prefix_name = f"{path.split('/')[1]}/formatted_prices/"
    objects = client.list_objects(BUCKET_NAME, prefix=prefix_name, recursive=True)
    for obj in objects:
        if obj.object_name.endswith('.csv'):
            return obj.object_name
    raise AirflowNotFoundException('The csv file does not exist')