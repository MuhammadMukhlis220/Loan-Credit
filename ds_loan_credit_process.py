from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pandas as pd
import uuid
from opensearchpy.helpers import bulk
from opensearchpy import OpenSearch
from sqlalchemy import create_engine
import psycopg2

today_date = datetime.now().strftime('%Y-%m-%d')

default_args = {
        'owner': 'mukhlis',
    'start_date': datetime(2025, 4, 29)
}

dag = DAG(
        'ds_loan_credit_process',
        default_args = default_args,
        catchup = False,
        schedule_interval = None,
        tags = ['dev', 'credit']
)

def to_opensearch():
    
    today = datetime.today().replace(hour=9, minute=0, second=0, microsecond=0) 
    yesterday = today - timedelta(days=1)

    engine = create_engine('postgresql+psycopg2://x:x@x.x.x.x/x')
    conn = None

    try:
        conn = engine.raw_connection()
        conn.autocommit = True

        query = f"""
        SELECT * FROM public.loan_credit
        WHERE insert_date BETWEEN '{yesterday}' AND '{today}'
        """

        df = pd.read_sql(query, con=conn)
        print(df.head())

    finally:
        if conn is not None:
            conn.close()
            
    OPENSEARCH_HOST = "x.x.x.x"
    OPENSEARCH_PORT = "x"
    OPENSEARCH_INDICES = ["ds_loan_credit", "ds_loan_credit_2"]
    OPENSEARCH_TYPE = "_doc"
    OPENSEARCH_URL = "https://x:x@x.x.x.x:x/"
    OPENSEARCH_CLUSTER = "ONYX-analytic"
    ONYX_OS = OpenSearch(
                         hosts = [{"host": OPENSEARCH_HOST, "port": OPENSEARCH_PORT}], http_auth = ("admin", "admin"),
                         use_ssl = True, verify_certs = False, ssl_assert_hostname = False, ssl_show_warn = False
                        )
    batch = 100000
    

    for x in range(0, len(df), batch):
        df = df.iloc[x:x+batch]
        for index_name in OPENSEARCH_INDICES:
            hits = [{"_op_type": "index", "_index": index_name, "_id": str(uuid.uuid4()), "_score": 1, "_source": i} for i in df.to_dict("records")]
            resp, err = bulk(ONYX_OS, hits, index=index_name, max_retries=3)
            print(resp, err)
    
    

t1 = SparkSubmitOperator(
    task_id='spark_run',
    application="/x/airflow/spark_job/loan_credit/ml_spark_test.py",
    name='spark_machine_learning',
    jars="file:///x/airflow/spark_job/postgresql-42.7.3.jar",
    dag=dag,
    yarn_queue="default"
    )

'''
t1 = BashOperator(
    task_id='run_spark_job',
    bash_command="""
        source /etc/airflow/conf/airflow-env.sh && \
        spark-submit --jars file:///usr/odp/0.2.0.0-04/airflow/spark_job/postgresql-42.7.3.jar \
        /usr/odp/0.2.0.0-04/airflow/spark_job/loan_credit/ml_spark_test.py
        """,
    dag = dag
)
'''

t2 = BashOperator(
    task_id = "dump_csv_hdfs_to_local",
    bash_command = f"""
    hdfs dfs -copyToLocal -f /loan_credit/data_applicant_today/data_applicant_{today_date}.csv /x/airflow/spark_job/loan_credit/
    """,
    dag=dag
)

t3 = PythonOperator(
    task_id = "to_opensearch",
    python_callable = to_opensearch,
    dag = dag
)

t1 >> t2 >> t3
