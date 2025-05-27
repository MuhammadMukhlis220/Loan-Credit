# Loan Credit
---

Work Flow:

![Alt Text](/pic/flow_chart.png)

Figure 1

## 1. Airflow

There will be 3 DAGs. There are [ds_loan_credit_add_data](/ds_loan_credit_add_data.py), [ds_loan_credit_process](/ds_loan_credit_process.py), and [ds_loan_credit](/ds_loan_credit.py).

![Alt Text](/pic/airflow_dag_1.png)

Figure 2

### 1.1 ds_loan_credit_add_data

This DAG is for generate credit applicant's data that will store in **PostgreSQL** .

### 1.2 ds_loan_credit_process

After the data is generated, **Apache Airflow** will orchestrate the next steps by triggering a **Spark** job to perform machine learning operations.
File used to execute this process is [ml_spark_test.py](/ml_spark_test.py)

Spark's code snapshot:

![Alt Text](/pic/spark_ml_code.png)

Figure 3

Spark's result:

![Alt Text](/pic/spark_ml_result.png)

Figure 4

Spark's CSV for local and hdfs:

![Alt Text](/pic/csv_result.png)

Figure 5

Csv's result:

![Alt Text](/pic/csv_result_2.png)

Figure 6

The workflow follows this sequence:

1. **Data Retrieval**  
   Spark will extract the required data from **PostgreSQL**.

2. **Data Deletion**  
   After retrieval, the source data in PostgreSQL will be **deleted** to ensure data consistency and manage storage.

3. **Machine Learning Execution**  
   Spark will run machine learning models on the extracted data.

4. **Exporting Results**  
   The machine learning results will be exported to four different destinations:
   - ✅ **PostgreSQL** (as processed data with ML results)
   - ✅ **Local storage** (in **CSV** format **/x/airflow/spark_job/loan_credit**)
   - ✅ **HDFS** (for further use in **Hive**. **/loan_credit/data_applicant_today**)
   - ✅ **OpenSearch** (for search and analytics purposes)

This pipeline ensures the data is processed, stored, and distributed efficiently across multiple systems for various downstream use cases.

### 1.3 ds_loan_credit

This DAG is for manage the DAG's order execution. `ds_loan_credit_add_data` first, then `ds_loan_credit_process`.

## 2. OpenSearch

Dashboard result and **automate update**:

![Alt Text](/pic/dashboard_1.png)

Figure 7
