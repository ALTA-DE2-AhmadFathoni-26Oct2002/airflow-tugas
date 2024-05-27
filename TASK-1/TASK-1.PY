from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def push_xcom_from_task(ti=None):
    ti.xcom_push(key='value1', value='nama saya ammar')
    ti.xcom_push(key='value2', value='saya belajar DE di Alterra')
    ti.xcom_push(key='value3', value='saya siap menjadi de')

def pull_xcom_from_task(ti=None):
    value1 = ti.xcom_pull(task_ids='push_xcom_from_task', key='value1')
    value2 = ti.xcom_pull(task_ids='push_xcom_from_task', key='value2')
    value3 = ti.xcom_pull(task_ids='push_xcom_from_task', key='value3')
    print(f'Perkenalkan {value1}, saat ini {value2}, artinya {value3}')

dag = DAG(
    'latihan_alterra_part1',
    description='Latihan Alterra Part 1',
    schedule_interval='* 5 * * *',
    start_date=datetime(2024, 5, 19),
    catchup=False
)

push_xcom = PythonOperator(
    task_id='push_xcom_from_task',
    python_callable=push_xcom_from_task,
    dag=dag
)

pull_xcom = PythonOperator(
    task_id='pull_xcom_from_task',
    python_callable=pull_xcom_from_task,
    dag=dag
)

push_xcom >> pull_xcom
