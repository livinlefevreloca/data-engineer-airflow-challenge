from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

import keyword_pipeline as k
import tempus_challenge as t
from values import key_words

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


keyword_dag = DAG('keyword_dag',
                  default_args=default_args,
                  schedule_interval=timedelta(days=1),
                  catchup=False
                  )

api_keyword_task = PythonOperator(
                   task_id='keyword_api',
                   provide_context=True,
                   python_callable=k.get_headlines,
                   params={"word_list": key_words,
                           "url": "https://newsapi.org/v2/top-headlines?q="},
                   dag=keyword_dag
                   )

tabularize_keyword_task = PythonOperator(
                             task_id="tabularize",
                             provide_context=True,
                             python_callable=k.tabularize,
                             dag=keyword_dag
                             )


upload_keyword_task = PythonOperator(
                      task_id='upload',
                      provide_context=True,
                      python_callable=t.upload,
                      dag=keyword_dag
                      )


end = DummyOperator(
    task_id='end',
    dag=keyword_dag
)


# set dependent tasks downs stream of there parent tasks.
api_keyword_task >> tabularize_keyword_task
tabularize_keyword_task >> upload_keyword_task
upload_keyword_task >> end
