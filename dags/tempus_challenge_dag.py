from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

import tempus_challenge as t

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 4, 1),
    'email': ['adamtlefevre@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}


tempus_challenge_dag = DAG('tempus_challenge_dag',
                           default_args=default_args,
                           schedule_interval=timedelta(days=1),
                           catchup=False
                           )

get_headlines_task = PythonOperator(
                     task_id='source_api',
                     provide_context=True,
                     python_callable=t.get_headlines,
                     params={"url":
                             "https://newsapi.org/v2/top-headlines?language=en"
                             },
                     dag=tempus_challenge_dag
                    )

tabularize_task = PythonOperator(
                  task_id='tabularize',
                  provide_context=True,
                  python_callable=t.tabularize,
                  retries=0,  # no I/O so nothing will be different on retry
                  dag=tempus_challenge_dag
)

upload_task = PythonOperator(
              task_id='upload',
              provide_context=True,
              python_callable=t.upload,
              dag=tempus_challenge_dag
)


end = DummyOperator(
    task_id='end',
    dag=tempus_challenge_dag
)


# set dependent tasks downs stream of there parent tasks.
get_headlines_task >> tabularize_task
tabularize_task >> upload_task
upload_task >> end
