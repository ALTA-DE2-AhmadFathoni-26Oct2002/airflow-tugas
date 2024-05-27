from datetime import datetime
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

dag = DAG (
    'tugas_aiflow_part2',
    schedule_interval= None,
    start_date= datetime(2024, 5, 21),
    catchup= False
)

identify_name = SimpleHttpOperator(
    task_id = "post_name",
    endpoint="/gender/by-first-name-multiple",
    method="POST",
    data= '{"country":"USA", "locale":null, "ip":null, "first_name":["sandra", "adi", "black"]}',
    http_conn_id="gender_api",
    log_response=True,
    dag=dag
)

create_table_in_db_task = PostgresOperator(
    task_id = 'create_table_in_task_id',
    sql = ('CREATE TABLE IF NOT EXISTS gender_name_prediction' +
    '(' +
       'input VARCHAR(50), ' +
       'details VARCHAR(50), ' +
       'result_found BOOLEAN, ' +
       'first_name VARCHAR(50), ' +
       'probability FLOAT(53), ' +
       'gender VARCHAR(50), ' +
       'timestamp TIMESTAMP WITHOUT TIME ZONE, ' +
    ')'),
    postgres_conn_id = 'pg_conn_id',
    autocommit = True,
    dag = dag
)

def loadDataToPostgres():
    pg_hook = PostgresHook(postgres_conn_id='pg_conn_id').get_conn()
    curr = pg_hook.cursor("cursor")
    with open (identify_name) as file:
        curr.copy_from(file, 'gender_name_prediction')
        pg_hook.commit()

load_data_to_db_task = PythonOperator(
    task_id = 'load_data_to_db',
    python_callable=loadDataToPostgres,
    dag=dag
)

identify_name >> create_table_in_db_task >> load_data_to_db_task
