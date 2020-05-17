"""
Basic ETL pipeline in Airflow.
"""
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from pprint import pprint

import psycopg2
import pandas as pd
import fastparquet
import hashlib

default_args = {
    "owner": "Bas Vonk",
    "depends_on_past": False,
    "start_date": days_ago(2),
    "email": ["sjj.vonk@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

dag = DAG("extract_transform_load", default_args=default_args, schedule_interval=timedelta(days=1))


def extractor(ds, **kwargs):

    filepath_raw = kwargs.get('filepath_raw')
    df = pd.read_csv(filepath_raw)
    df.to_parquet('/staging-area/hospital_a_2020_05_16_loaded.parquet')

    kwargs['ti'].xcom_push(key='filepath_loaded',
                           value='/staging-area/hospital_a_2020_05_16_loaded.parquet')

    return 'Extraction succesful. Parquet file stored an availabe in XCom.'


def transformer(ds, **kwargs):

    filepath_loaded = kwargs['ti'].xcom_pull(key='filepath_loaded',
                                             task_ids='extract')

    df = pd.read_parquet(filepath_loaded)
    df = df[df['action'] == 'admission'][['patient_id', 'first_name', 'last_name', 'timestamp']].merge(df[df['action'] == 'discharge'][['patient_id', 'timestamp']], how='left', on='patient_id')
    df = df.rename(columns={'timestamp_x': 'admission_timestamp',
                            'timestamp_y': 'discharge_timestamp'})

    df['admission_id'] = df.apply(lambda x: hashlib.sha256(str(str(x['patient_id']) + x['admission_timestamp']).encode('utf-8')).hexdigest(), axis=1)

    df.to_parquet('/staging-area/hospital_a_2020_05_16_transformed.parquet')

    kwargs['ti'].xcom_push(key='filepath_transformed',
                           value='/staging-area/hospital_a_2020_05_16_transformed.parquet')

    return 'Transformation succesful.'


def loader(ds, **kwargs):

    filepath_extracted = kwargs['ti'].xcom_pull(key='filepath_transformed',
                                                task_ids='transform')

    df = pd.read_parquet(filepath_extracted)

    conn = psycopg2.connect("dbname='data_warehouse' user='username' host='data-warehouse' password='password'")

    df_insert = df[['admission_id', 'patient_id', 'admission_timestamp', 'discharge_timestamp',
                    'patient_id', 'admission_timestamp', 'discharge_timestamp']]

    # Using executemany is the second fastest insert option after copy_from, but being able
    # to do and UPSERT in ONE transaction also saves a lot of roundtrips to the database
    cur = conn.cursor()
    cur.executemany(
        """
        INSERT INTO data_warehouse.admissions (admission_id, patient_id, admission_timestamp, discharge_timestamp)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (admission_id) DO UPDATE
            SET patient_id = %s,
                admission_timestamp = %s,
                discharge_timestamp = %s
        """,
        list(df_insert.itertuples(index=False, name=None))
    )
    conn.commit()

    return 'Loading succesful.'


# t1, t2 and t3 are examples of tasks created by instantiating operators
extract = PythonOperator(
    task_id='extract',
    provide_context=True,
    python_callable=extractor,
    dag=dag,
    op_kwargs={'filepath_raw': '/data-lake/hospital_a_2020_05_16.csv'}
)

transform = PythonOperator(
    task_id='transform',
    provide_context=True,
    python_callable=transformer,
    dag=dag
)

load = PythonOperator(
    task_id='load',
    provide_context=True,
    python_callable=loader,
    dag=dag
)

extract >> transform >> load
