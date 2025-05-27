from airflow import DAG
from airflow.decorators import task
from datetime import datetime
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils import timezone
import pendulum

with DAG(
    'ds_loan_credit',
    default_args={
        'start_date': datetime(2025, 5, 1)
    },
    schedule_interval='0 9 * * *',
    catchup=False,
    tags=['prod', 'loan_credit']
) as dag:
    
    t1 = TriggerDagRunOperator(
        task_id='ds_loan_credit_add_data',
        trigger_dag_id='ds_loan_credit_add_data',
        wait_for_completion=True,
        trigger_rule=TriggerRule.ALL_SUCCESS
    )
    
    t2 = TriggerDagRunOperator(
        task_id='ds_loan_credit_process',
        trigger_dag_id='ds_loan_credit_process',
        wait_for_completion=True,
        trigger_rule=TriggerRule.ALL_SUCCESS
    )
    
t1 >> t2
