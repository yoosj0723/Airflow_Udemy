from airflow.hooks.base import BaseHook 
import requests
import json
from minio import Minio
from io import BytesIO

def _get_stock_prices(url, symbol):
    url = f"{url}{symbol}?metrics=high?&interval=1d&range=1y"
    api = BaseHook.get_connection('stock_api')
    response = requests.get(url, headers=api.extra_dejson['headers'])
    return json.dumps(response.json()['chart']['result'][0])

def _store_prices(stock):
    minio=BaseHook.get_connection('minio')#.extra_dejson
    print(minio)
    client = Minio(         #client를 통해 minio와 연결
        endpoint=minio.extra_dejson['endpoint_url'].split('//')[1],
        access_key=minio.login,
        secret_key=minio.password,
        secure=False
    )
    bucket_name = 'stock-market'
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
    stock = json.loads(stock)               #a문자열을 Python dictionary로 변환
    symbol = stock['meta']['symbol']        #stock은 XComs로 갖고 와야 하므로 parmeter설정해야됨
    data = json.dumps(stock, ensure_ascii=False).encode('utf8')
    objw = client.put_object(
        bucket_name=bucket_name,
        object_name=f'{symbol}/prices.json',     #bucket에 symbol폴더 생성 후 prices.json파일 생성
        data=BytesIO(data),
        length=len(data)
    )
    return f'{objw.bucket_name}/{symbol}'