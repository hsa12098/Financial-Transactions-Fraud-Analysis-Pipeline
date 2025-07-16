# dags/financial_pipeline_dag.py
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
#from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'financial_transactions_pipeline',
    default_args=default_args,
    description='Financial Transactions Data Engineering Pipeline',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['financial', 'transactions', 'fraud_detection'],
) as dag:

    # Task to check if services are running
    check_services = BashOperator(
        task_id='check_services',
        bash_command="""
            echo 'Checking services...'
            if ! nc -z localhost 3306; then
                echo 'MySQL is not running'
                exit 1
            fi
            if ! nc -z localhost 9092; then
                echo 'Kafka is not running'
                exit 1
            fi
            echo 'All services are running'
        """,
    )

    # Task to initialize MySQL database
    init_database = BashOperator(
        task_id='init_database',
        bash_command="""
            mysql -u admin -pxyx -e "CREATE DATABASE IF NOT EXISTS finance_db;" && 
            mysql -u admin -pxyz finance_db < /home/hamza/DE/20212619_DS463_final/financial-transactions-project/sql/create_mysql_tables.sql
        """,
    )


    # Task to stream Kaggle fraud data to Kafka
    stream_kaggle_data = BashOperator(
        task_id='stream_kaggle_data',
        bash_command='python /home/hamza/DE/20212619_DS463_final/financial-transactions-project/kafka/producer1.py',
    )

    # Task to stream bank simulation data to Kafka
    stream_bank_data = BashOperator(
        task_id='stream_bank_data',
        bash_command='python /home/hamza/DE/20212619_DS463_final/financial-transactions-project/kafka/producer2.py',
    )

    # Task to process data with Spark
    process_with_spark = BashOperator(
        task_id='process_with_spark',
        bash_command='timeout 120 spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,mysql:mysql-connector-java:8.0.28 /home/hamza/DE/20212619_DS463_final/financial-transactions-project/spark/consumer_mysql.py',
        retries=0, 
    )

    # Task to generate fraud detection report
    # Task to generate a comprehensive fraud detection report with separate dataset visualizations

    generate_report = BashOperator(
    task_id='generate_report',
    bash_command='python /home/hamza/DE/20212619_DS463_final/financial-transactions-project/scripts/generate_report.py',
    trigger_rule='all_done',
)


    # Define task dependencies
    check_services >> init_database >> [stream_kaggle_data, stream_bank_data] >> process_with_spark >> generate_report
