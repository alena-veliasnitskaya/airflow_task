from __future__ import annotations

import datetime
import os
from glob import glob
from typing import Sequence
from datetime import timedelta
import pandas as pd
import pymongo

from airflow.hooks.filesystem import FSHook
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.models.xcom import XCom
from airflow.providers.mongo.hooks.mongo import MongoHook



default_args = {
    'owner': 'alena',
    'start_date': days_ago(1),
    'depends_on_past': False,
}


class FileSensor(BaseSensorOperator):
    template_fields: Sequence[str] = ("filepath",)
    ui_color = "#91818a"
    def __init__(self, filepath, fs_conn_id="my_file_system", recursive=False, *args, **kwargs):
        super(FileSensor, self).__init__(*args, **kwargs)
        self.filepath = filepath
        self.fs_conn_id = fs_conn_id
        self.recursive = recursive
    """
    Waits for a file or folder to land in a filesystem.

    If the path given is a directory then this sensor will only return true if
    any files exist inside it (either directly, or within a subdirectory)

    """
    def poke(self, context: Context):
        hook = FSHook(self.fs_conn_id)
        basepath = hook.get_path()
        full_path = os.path.join(basepath, self.filepath)
        self.log.info("Poking for file %s", full_path)

        for path in glob(full_path, recursive=self.recursive):
            if os.path.isfile(path):
                mod_time = datetime.datetime.fromtimestamp(os.path.getmtime(path)).strftime("%Y%m%d%H%M%S")
                self.log.info("Found File %s last modified: %s", str(path), mod_time)
                return True

            for _, _, files in os.walk(path):
                if len(files) > 0:
                    return True
        return False
def say_hi(**context):
    print('Hello, I found your file!')

def null_values(**context):
    data = pd.read_csv("/mnt/d/airflow_project/tiktok_google_play_reviews.csv")
    df = data.copy()
    df = df.fillna('-')
    context['ti'].xcom_push(key='df', value=df.to_dict())
    print('everything is okay')

def sort_value(**context):
    df = context.get('ti').xcom_pull(key='df')
    df1 = pd.DataFrame.from_dict(df)
    df1.sort_values(by='at', ascending=False)
    context['ti'].xcom_push(key='df1', value=df1.to_dict())
    print('everything is okay')
    return df1


def remove_emojis(**context):
    df1 = context.get('ti').xcom_pull(key='df1')
    df2 = pd.DataFrame.from_dict(df1)
    df2.replace({r'[^\x00-\x7F]+':''}, regex=True, inplace=True)
    context['ti'].xcom_push(key='df2', value=df2.to_dict())
    print('everything is okay')
    return df2

def uploadtomongo(**context):
    collection_data = context.get('ti').xcom_pull(key='df2')
    try:
        hook = MongoHook(mongo_conn_id='mongo_default')
        client = hook.get_conn()
        mydb = client.admin
        print(f"Connected to MongoDB - {client.server_info()}")
        mycol = mydb["new_col"]
        data = mycol.insert_many(collection_data)
    except Exception as e:
        print(f"Error connecting to MongoDB -- {e}")

with DAG(
    'my_sensor_dag',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False
) as dag:
    sensing_task = FileSensor(task_id = 'sensing_task',
                              fs_conn_id="my_file_system",
                              filepath='tiktok_google_play_reviews.csv',
                              poke_interval = 10)
    check = PythonOperator(task_id = 'check_sensor',
                           python_callable = say_hi,
                           provide_context = True,
                           retries = 10,
                           retry_delay = timedelta(seconds = 1))
    with TaskGroup('data_transformation')  as processing_task:
        task1 = PythonOperator(task_id = 'null',
                           python_callable = null_values,
                           provide_context = True)
        task2 =PythonOperator(task_id = 'sort',
                           python_callable = sort_value,
                           provide_context = True)
        task3 =PythonOperator(task_id = 'emoji',
                           python_callable = remove_emojis,
                           provide_context = True)
        task1 >> task2 >> task3
    load_task =  PythonOperator(
        task_id='upload-mongodb',
        python_callable=uploadtomongo,
        provide_context=True
    )

    sensing_task >> check >> processing_task >> load_task