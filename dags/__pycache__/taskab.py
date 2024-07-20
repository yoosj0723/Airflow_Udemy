from airflow.decorators import dag, task
from datetime import datetime

@dag(
    start_date=datetime(2023, 1, 1),    #시작 날짜
    schedule='@daily',                  #trigger 빈도 = 매일 자정
    catchup=False,                      #비 트리거 상태의 Dag 런은 실행 X
    tags=['taskflow']               #Airflow UI의 데이터 파이프라인을 필터링하는덷 유용해서 함
)
def taskflow():

    @task
    def task_a():
        print("Task A")
        return 42
    
    @task
    def task_b(value):
        print("Task B")
        print(value)

    task_b(task_a())

taskflow()