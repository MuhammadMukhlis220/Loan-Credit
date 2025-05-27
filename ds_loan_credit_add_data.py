from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
import pandas as pd
import random
import uuid
import psycopg2
from sqlalchemy import create_engine
import json

default_args = {
        'owner': 'mukhlis',
    'start_date': datetime(2025, 4, 29)
}

dag = DAG(
        'ds_loan_credit_add_data',
        default_args = default_args,
        catchup = False,
        schedule_interval = None,
        tags = ['dev', 'credit']
)

def create_data(ti):
    
    random.seed()

    def generate_applicant():
        income = round(random.uniform(10, 50), 2)  # juta IDR
        loan_amount = round(random.uniform(600, 4950), 2)  # juta IDR
        loan_term = random.choice([60, 120, 180, 240])  # bulan 60, 
        credit_score = random.randint(300, 700)
        age = random.randint(27, 46)
        employment_status = random.choice(['permanent', 'contract']) #  
        marital_status = random.choice(['single', 'married'])
        number_of_dependents = random.randint(0, 3)
        education_level = random.choice(['bachelor', 'master'])# 'high_school', 
        property_area = random.choice(['urban', 'semiurban', 'rural'])# 
        has_prior_loans = random.choice([True, False]) #
        prior_loan_defaults = random.randint(0, 2) if has_prior_loans else 0
        '''
        monthly_installment = loan_amount / loan_term
        prob = 0.1

        if income < monthly_installment * 1.5:
            prob += 0.4
        if credit_score < 600:
            prob += 0.2
        if prior_loan_defaults > 0:
            prob += 0.3
        if number_of_dependents >= 3 and income < 10:
            prob += 0.2
        if income > 50:
            prob -= 0.3

        prob = min(max(prob, 0), 1)
        approval_status = 1 if random.random() > prob else 0
        '''
        today = datetime.now().replace(hour=9, minute=0, second=0, microsecond=0) 
        yesterday = today - timedelta(days=1)

        start_date = yesterday
        end_date = today

        insert_date = start_date + (end_date - start_date) * random.random()

        return {
            "applicant_id": str(uuid.uuid4())[:8],
            "age": age,
            "income": income,
            "loan_amount": loan_amount,
            "loan_term": loan_term,
            "credit_score": credit_score,
            "employment_status": employment_status,
            "marital_status": marital_status,
            "number_of_dependents": number_of_dependents,
            "education_level": education_level,
            "property_area": property_area,
            "has_prior_loans": has_prior_loans,
            "prior_loan_defaults": prior_loan_defaults,
           # "approval_status": approval_status,
            "insert_date": insert_date
        }

    num_rows = random.randint(10, 75)
    data = [generate_applicant() for _ in range(num_rows)]
    df = pd.DataFrame(data)

    df['insert_date'] = pd.to_datetime(df['insert_date'])
    df['insert_date'] = df['insert_date'].dt.strftime('%Y-%m-%dT%H:%M:%S.%f')
    
    print(f"Generated {len(df)} rows")
    print(df.head(10))
    
    ti.xcom_push(key='df_from_create_data', value=df.to_json(orient='records'))
    
def to_postgre(ti):
    
    df_json = ti.xcom_pull(task_ids='create_data', key='df_from_create_data')
    df = pd.read_json(df_json, orient='records')
    df['insert_date'] = pd.to_datetime(df['insert_date'])
    
    print(df.head(2))
    
    df['insert_date'] = df['insert_date'].astype(str)
    
    conn = psycopg2.connect(
        host="x.x.x.x",
        database="x",
        user="x",
        password="x"
    )
    
    try:
        cur = conn.cursor()

        postgre_table = 'loan_credit_2'

        insert_query = f"INSERT INTO public.{postgre_table} ({', '.join(df.columns)}) VALUES "

        insert_values = ", ".join([repr(tuple(r.tolist())) for _, r in df.iterrows()])

        final_query = insert_query + insert_values

        cur.execute(final_query)

        conn.commit()
        print(f"Done, masuk ke PostgreSQL tabel {postgre_table}")

    except Exception as e:
        print(f"Error saat memasukkan data ke PostgreSQL: {e}")
        
    finally:
        cur.close()
        conn.close()
        
t1 = PythonOperator(
    task_id = "create_data",
    python_callable = create_data,
    dag = dag
)

t2 = PythonOperator(
    task_id = "to_postgre",
    python_callable = to_postgre,
    dag = dag
)

t1 >> t2
