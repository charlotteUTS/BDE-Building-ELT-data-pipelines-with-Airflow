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
    dag_id='at3_part1',
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
Census_G01_NSW_LGA = AIRFLOW_DATA+"/Census LGA/2016Census_G01_NSW_LGA.csv"
Census_G02_NSW_LGA = AIRFLOW_DATA+"/Census LGA/2016Census_G02_NSW_LGA.csv"
NSW_LGA_CODE = AIRFLOW_DATA+"/NSW_LGA/NSW_LGA_CODE.csv"
NSW_LGA_SUBURB = AIRFLOW_DATA+"/NSW_LGA/NSW_LGA_SUBURB.csv"
LISTING = AIRFLOW_DATA+"/listings/05_2020.csv"

#########################################################
#
#   Custom Logics for Operator
#
#########################################################


def import_load_NSW_LGA_CODE_func(**kwargs):

    #set up pg connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    #get all files with filename including the string '.csv'
    #filelist = [k for k in os.listdir(NSW_LGA_CODE) if '.csv' in k]

    #generate dataframe by combining files
    #df = pd.concat([pd.read_csv(os.path.join(NSW_LGA_CODE, fname)) for fname in filelist], ignore_index=True)
    df = pd.read_csv(NSW_LGA_CODE)

    if len(df) > 0:
        col_names = ['LGA_CODE','LGA_NAME']															
        values = df[col_names].to_dict('split')
        values = values['data']
        logging.info(values)
	
        insert_sql = """
                    INSERT INTO "RAW".NSW_LGA_CODE(LGA_CODE,LGA_NAME)
                    VALUES %s
                    """

        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
        conn_ps.commit()
    else:
        None

    return None


def import_load_NSW_LGA_SUBURB_func(**kwargs):

    #set up pg connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    #generate dataframe by combining files
    df = pd.read_csv(NSW_LGA_SUBURB)

    if len(df) > 0:
        col_names = ['LGA_NAME','SUBURB_NAME']															
        values = df[col_names].to_dict('split')
        values = values['data']
        logging.info(values)
	
        insert_sql = """
                    INSERT INTO "RAW".nsw_lga_suburb(LGA_NAME,SUBURB_NAME)
                    VALUES %s
                    """

        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
        conn_ps.commit()
    else:
        None

    return None


def import_load_Census_G01_NSW_LGA_func(**kwargs):

    #set up pg connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    #get all files with filename including the string '.csv'
    #filelist = [k for k in os.listdir(NSW_LGA_CODE) if '.csv' in k]

    #generate dataframe by combining files
    #df = pd.concat([pd.read_csv(os.path.join(NSW_LGA_CODE, fname)) for fname in filelist], ignore_index=True)
    df = pd.read_csv(Census_G01_NSW_LGA) 

    if not df.empty:
        # Define the list of column names based on the columns in the DataFrame
        col_names = list(df.columns)

        # Extract values from the DataFrame
        values = df.values.tolist()

        # Generate the INSERT SQL statement dynamically
        insert_sql = f"""
            INSERT INTO "RAW".Census_G01_NSW_LGA ({", ".join(col_names)})
            VALUES %s
        """

        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
        conn_ps.commit()
    else:
        None

    return None


def import_load_Census_G02_NSW_LGA_func(**kwargs):

    #set up pg connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    #get all files with filename including the string '.csv'
    df = pd.read_csv(Census_G02_NSW_LGA)

    if not df.empty:
        # Define the list of column names based on the columns in the DataFrame
        col_names = list(df.columns)

        # Extract values from the DataFrame
        values = df.values.tolist()

        # Generate the INSERT SQL statement dynamically
        insert_sql = f"""
            INSERT INTO "RAW".Census_G02_NSW_LGA ({", ".join(col_names)})
            VALUES %s
        """

        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
        conn_ps.commit()
    else:
        None

    return None


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
            INSERT INTO "RAW".LISTINGS ({", ".join(col_names)})
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

import_load_Census_G01_NSW_LGA_task = PythonOperator(
    task_id="import_load_Census_G01_NSW_LGA",
    python_callable=import_load_Census_G01_NSW_LGA_func,
    op_kwargs={},
    provide_context=True,
    dag=dag
)
import_load_Census_G02_NSW_LGA_task = PythonOperator(
    task_id="import_load_Census_G02_NSW_LGA",
    python_callable=import_load_Census_G02_NSW_LGA_func,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

import_load_NSW_LGA_SUBURB_task = PythonOperator(
    task_id="import_load_NSW_LGA_SUBURB",
    python_callable=import_load_NSW_LGA_SUBURB_func,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

import_load_NSW_LGA_CODE_task = PythonOperator(
    task_id="import_load_NSW_LGA_CODE",
    python_callable=import_load_NSW_LGA_CODE_func,
    op_kwargs={},
    provide_context=True,
    dag=dag
)






import_load_listings_task,import_load_NSW_LGA_CODE_task, import_load_NSW_LGA_SUBURB_task, import_load_Census_G02_NSW_LGA_task, import_load_Census_G01_NSW_LGA_task