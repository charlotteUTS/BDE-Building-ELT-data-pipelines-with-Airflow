import os
import logging
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from psycopg2.extras import execute_values
from airflow import AirflowException
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


dag_default_args = {
    'owner': 'at3',
    'start_date': datetime.now() - timedelta(days=2+4),
    'email': [],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'wait_for_downstream': False,
}

dag = DAG(
    dag_id='at3_part1_082020',
    default_args=dag_default_args,
    schedule_interval=None,
    catchup=True,
    max_active_runs=1,
    concurrency=5
)

#########################################################
#
#   Load Environment Variables
#
#########################################################
AIRFLOW_DATA = "/home/airflow/gcs/data"
LISTING = AIRFLOW_DATA+"/listings/08_2020.csv"

#########################################################
#
#   Custom Logics for Operator
#
#########################################################



def import_load_listings_func(**kwargs):

    #set up pg connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    #get all files with filename including the string '.csv'
    df = pd.read_csv(LISTING)

    if not df.empty:
        # Define the list of column names based on the columns in the DataFrame
        col_names = list(df.columns)

        # Extract values from the DataFrame
        values = df.values.tolist()

        # Generate the INSERT SQL statement dynamically
        insert_sql = f"""
            INSERT INTO raw.LISTINGS ({", ".join(col_names)})
            VALUES %s
        """

        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
        conn_ps.commit()
    else:
        None

    return None



#########################################################
#
#   DAG Operator Setup
#
#########################################################

import_load_listings_task = PythonOperator(
    task_id="import_load_listings",
    python_callable=import_load_listings_func,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

import_load_listings_task