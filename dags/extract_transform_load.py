"""Basic ETL pipeline in Airflow."""
import os
from datetime import timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import psycopg2
import pandas as pd
import fastparquet
import hashlib

OPERATOR_KWARGS = {'filename': 'hospital_a_2020_05_16',
                   'extension_raw_data': '.csv'}

TASK_ID_EXTRACT = 'extract'
TASK_ID_TRANSFORM = 'transform'
TASK_ID_LOAD = 'load'

DATA_LAKE_PATH = '/data-lake/'
STAGING_AREA_PATH = '/staging-area/'

XCOM_KEY_LOADED_DATA = 'filepath_loaded'
XCOM_KEY_TRANSFORMED_DATA = 'filepath_transformed'


def get_data_raw_file_path(filename, extension):
    return os.path.join(DATA_LAKE_PATH, f"{filename}{extension}")


def get_data_loaded_path(filename):
    return os.path.join(STAGING_AREA_PATH, f"{filename}_loaded.parquet")


def get_data_transformed_path(filename):
    return os.path.join(STAGING_AREA_PATH, f"{filename}_transformed.parquet")


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

dag = DAG("extract_transform_load", default_args=default_args,
          schedule_interval=timedelta(days=1))

# This DAG does the following:
# 1. Extract the data from a data lake (in this case it's just a folder with CSVs) and store it in
#    Parquet files
# 2. Transform the data into a format that we want in the data warehouse (in this case this means
#    generating admissions from a plain list with events)
# 3. Load the the data in the data warehouse


def extractor(ds, **kwargs):

    filename = kwargs['filename']
    extension_raw_data = kwargs['extension_raw_data']
    data_raw = pd.read_csv(get_data_raw_file_path(filename, extension_raw_data))

    # WARNING: This can only be done when all tasks will be run on the same host
    # Typically this is not the case and you want to use S3 or HDFS to store intermediate results
    data_raw.to_parquet(get_data_loaded_path(filename))

    # Push the location of the intermediate data to the XCom
    kwargs['ti'].xcom_push(key=XCOM_KEY_LOADED_DATA,
                           value=get_data_loaded_path(filename))

    return 'Extraction succesful. Parquet file stored an availabe in XCom.'


def transformer(ds, **kwargs):

    # Get the filepath for the loaded data and load the data to dataframe
    filename = kwargs['filename']
    filepath_loaded = kwargs['ti'].xcom_pull(key=XCOM_KEY_LOADED_DATA,
                                             task_ids=TASK_ID_EXTRACT)
    data_loaded = pd.read_parquet(filepath_loaded)

    # Do the transformations
    columns_left = ['patient_id', 'first_name', 'last_name', 'timestamp']
    columns_right = ['patient_id', 'timestamp']  # Column to join on and the discharge_timestamp
    data_transformed = data_loaded[data_loaded['action'] == 'admission'][columns_left] \
        .merge(data_loaded[data_loaded['action'] == 'discharge'][columns_right], 'left', 'patient_id')  # noqa
    data_transformed = data_transformed.rename(columns={'timestamp_x': 'admission_timestamp',
                                                        'timestamp_y': 'discharge_timestamp'})

    # Create admission ID based on fixed elements to ensure idempotence
    data_transformed['admission_id'] = data_transformed.apply(lambda x: hashlib.sha256(
        str(str(x['patient_id']) + x['admission_timestamp']).encode('utf-8')).hexdigest(), axis=1)

    # WARNING: This can only be done when all tasks will be run on the same host
    # Typically this is not the case and you want to use S3 or HDFS to store intermediate results
    data_transformed.to_parquet(get_data_transformed_path(filename))
    os.remove(filepath_loaded)

    # Push the location of the intermediate data to the XCom
    kwargs['ti'].xcom_push(key=XCOM_KEY_TRANSFORMED_DATA,
                           value=get_data_transformed_path(filename))

    return 'Transformation succesful.'


def loader(ds, **kwargs):

    filename = kwargs['filename']
    filepath_transformed = kwargs['ti'].xcom_pull(key=XCOM_KEY_TRANSFORMED_DATA,
                                                  task_ids=TASK_ID_TRANSFORM)
    data_transformed = pd.read_parquet(filepath_transformed)

    # Create psycopg2 connection
    conn = psycopg2.connect(f"dbname={os.getenv('DATA_WAREHOUSE_DB')} "
                            f"user={os.getenv('DATA_WAREHOUSE_USER')} "
                            f"host={os.getenv('DATA_WAREHOUSE_HOST')} "
                            f"password={os.getenv('DATA_WAREHOUSE_PASSWORD')}")

    # The order of the columns in this dataframe match the parameters in the query
    df_insert = data_transformed[['admission_id', 'patient_id', 'admission_timestamp',
                                  'discharge_timestamp', 'patient_id', 'admission_timestamp',
                                  'discharge_timestamp']]

    # Using executemany is the second fastest insert option after copy_from, but being able
    # to do and UPSERT in ONE transaction also saves a lot of roundtrips to the database
    cur = conn.cursor()
    cur.executemany(
        """
        INSERT INTO data_warehouse.admissions
            (admission_id, patient_id, admission_timestamp, discharge_timestamp)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (admission_id) DO UPDATE
            SET patient_id = %s,
                admission_timestamp = %s,
                discharge_timestamp = %s
        """,
        list(df_insert.itertuples(index=False, name=None))
    )

    # Commit the results in the DB
    conn.commit()

    # CLean up
    os.remove(filepath_transformed)

    return 'Loading succesful.'


# t1, t2 and t3 are examples of tasks created by instantiating operators
extract = PythonOperator(
    task_id=TASK_ID_EXTRACT,
    provide_context=True,
    python_callable=extractor,
    dag=dag,
    op_kwargs=OPERATOR_KWARGS
)

transform = PythonOperator(
    task_id=TASK_ID_TRANSFORM,
    provide_context=True,
    python_callable=transformer,
    dag=dag,
    op_kwargs=OPERATOR_KWARGS
)

load = PythonOperator(
    task_id=TASK_ID_LOAD,
    provide_context=True,
    python_callable=loader,
    dag=dag,
    op_kwargs=OPERATOR_KWARGS
)

extract >> transform >> load
