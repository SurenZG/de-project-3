# Проект 3

## Александр, здравствуй! 
## Разумеется, давай на ты :)

## Да, я совсем не учел запросы. 
## Сейчас я вложил их в методы, т.к. код не очень большой.
## Такой вариант написания приветствуется? 
```
import time
import requests
import json
import pandas as pd
import numpy as np

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.http.hooks.http import HttpHook


api_key = '5f55e6c0-e9e5-4a9c-b313-63c01fc31460'
base_url = 'https://d5dg1j9kt695d30blp03.apigw.yandexcloud.net'

postgres_conn_id = 'postgresql_de'

nickname = 'Suren'
cohort = '2'

headers = {
    'X-Nickname': nickname,
    'X-Cohort': cohort,
    'X-Project': 'True',
    'X-API-KEY': api_key,
    'Content-Type': 'application/x-www-form-urlencoded'
}

def add_column():
 
    postgres_hook = PostgresHook(postgres_conn_id)                          
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()                                
    cursor.execute('''
    DO $$ BEGIN
        IF (select max(date_time) :: date from staging.user_order_log) = (select (now() - INTERVAL '1 DAY') :: date)
        THEN
        truncate table staging.user_order_log RESTART IDENTITY;
        truncate table mart.f_sales RESTART IDENTITY;
        END IF;
    END$$; 
    ''')
    cursor.execute('''
    alter table staging.user_order_log add IF NOT EXISTS status varchar(50);
    ''')
    conn.commit()


def generate_report(ti):
    print('Making request generate_report')

    response = requests.post(f'{base_url}/generate_report', headers=headers)
    response.raise_for_status()
    task_id = json.loads(response.content)['task_id']
    ti.xcom_push(key='task_id', value=task_id)
    print(f'Response is {response.content}')


def get_report(ti):
    print('Making request get_report')
    task_id = ti.xcom_pull(key='task_id')

    report_id = None

    for i in range(20):
        response = requests.get(f'{base_url}/get_report?task_id={task_id}', headers=headers)
        response.raise_for_status()
        print(f'Response is {response.content}')
        status = json.loads(response.content)['status']
        if status == 'SUCCESS':
            report_id = json.loads(response.content)['data']['report_id']
            break
        else:
            time.sleep(10)

    if not report_id:
        raise TimeoutError()

    ti.xcom_push(key='report_id', value=report_id)
    print(f'Report_id={report_id}')


def get_increment(date, ti):
    print('Making request get_increment')
    report_id = ti.xcom_pull(key='report_id')
    response = requests.get(
        f'{base_url}/get_increment?report_id={report_id}&date={str(date)}T00:00:00',
        headers=headers)
    response.raise_for_status()
    print(f'Response is {response.content}')

    increment_id = json.loads(response.content)['data']['increment_id']
    ti.xcom_push(key='increment_id', value=increment_id)
    print(f'increment_id={increment_id}')


def upload_data_to_staging(filename, date, pg_table, pg_schema, ti):
    increment_id = ti.xcom_pull(key='increment_id')
    s3_filename = f'https://storage.yandexcloud.net/s3-sprint3/cohort_{cohort}/{nickname}/project/{increment_id}/{filename}'

    local_filename = date.replace('-', '') + '_' + filename

    response = requests.get(s3_filename)
    open(f"{local_filename}", "wb").write(response.content)

    df = pd.read_csv(local_filename)
    df = df.drop(['id'], axis=1)
    df = df.drop_duplicates().reset_index(drop=True)
    df['payment_amount'] = np.where((df['status'] == 'refunded'), - df['payment_amount'], df['payment_amount'])
    df['quantity'] = np.where((df['status'] == 'refunded'), - df['quantity'], df['quantity'])


    postgres_hook = PostgresHook(postgres_conn_id)
    engine = postgres_hook.get_sqlalchemy_engine()
    df.to_sql(pg_table, engine, schema=pg_schema, if_exists='append', index=False)


args = {
    "owner": "student",
    'email': ['student@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

business_dt = '{{ ds }}'

with DAG(
        'customer_retention',
        default_args=args,
        description='Provide default dag for sprint3',
        catchup=True,
        start_date=datetime.today() - timedelta(days=8),
        end_date=datetime.today() - timedelta(days=1),
) as dag:

    add_column = PythonOperator(
            task_id='add_column',
            python_callable=add_column)

    generate_report = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report)

    get_report = PythonOperator(
        task_id='get_report',
        python_callable=get_report)

    get_increment = PythonOperator(
        task_id='get_increment',
        python_callable=get_increment,
        op_kwargs={'date': business_dt})

    upload_user_order_inc = PythonOperator(
        task_id='upload_user_order_inc',
        python_callable=upload_data_to_staging,
        op_kwargs={'date': business_dt,
                   'filename': 'user_orders_log_inc.csv',
                   'pg_table': 'user_order_log',
                   'pg_schema': 'staging'})

    update_d_item_table = PostgresOperator(
        task_id='update_d_item',
        postgres_conn_id=postgres_conn_id,
        sql='''insert into mart.d_item (item_id, item_name)
            select item_id, item_name from staging.user_order_log
            where item_id not in (select item_id from mart.d_item)
            group by item_id, item_name'''
)

    update_d_customer_table = PostgresOperator(
        task_id='update_d_customer',
        postgres_conn_id=postgres_conn_id,
        sql='''insert into mart.d_customer (customer_id, first_name, last_name, city_id)
                select customer_id, first_name, last_name, max(city_id) from staging.user_order_log
                where customer_id not in (select customer_id from mart.d_customer)
                group by customer_id, first_name, last_name'''
)

    update_d_city_table = PostgresOperator(
        task_id='update_d_city',
        postgres_conn_id=postgres_conn_id,
        sql='''insert into mart.d_city (city_id, city_name)
                select city_id, city_name from staging.user_order_log
                where city_id not in (select city_id from mart.d_city)
                group by city_id, city_name;'''
    )

    update_f_sales = PostgresOperator(
        task_id='update_f_sales',
        postgres_conn_id=postgres_conn_id,
        sql='''insert into mart.f_sales (date_id, item_id, customer_id, city_id, quantity, payment_amount) 
            select dc.date_id, item_id, customer_id, city_id, quantity, payment_amount from staging.user_order_log uol 
            left join mart.d_calendar as dc on uol.date_time::Date = dc.date_actual 
            where uol.date_time::Date = '{{ds}}';''',
        parameters={"date": {business_dt}}
    )

    update_f_customer_retention = PostgresOperator(
        task_id='update_f_customer_retention',
        postgres_conn_id=postgres_conn_id,
        sql='''drop table if exists mart.f_customer_retention;
                create table mart.f_customer_retention
                as 
                (with cus as (select date_part('week',date_time) weekly
                                , date_part('month',date_time) monthly
                                , customer_id 
                                , quantity 
                                , payment_amount 
                                , case when count(quantity) over(partition by  customer_id ,date_part('week',date_time)) = 1 then 'new' else status end status
                                from staging.user_order_log uol )
 
                select count(distinct case when status = 'new' then customer_id end) new_customers_count
                ,count(distinct case when status != 'new' then customer_id end) returning_customers_count 
                ,count(distinct case when status = 'refunded' then customer_id end) refunded_customer_count
                ,weekly
                ,monthly
                ,sum(case when status = 'new' then payment_amount end) new_customers_revenue 
                ,sum(case when status != 'new' then payment_amount end) returning_customers_revenue  
                ,sum(case when status = 'refunded' then quantity end) customers_refunded   
                from cus
                group by weekly,monthly);'''
    )

    (
            add_column

            >>generate_report
            >> get_report
            >> get_increment
            >> upload_user_order_inc
            >> [update_d_item_table, update_d_city_table, update_d_customer_table]
            >> update_f_sales
            >> update_f_customer_retention
    )


```
