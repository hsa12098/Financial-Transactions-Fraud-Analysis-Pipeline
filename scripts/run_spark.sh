
#!/bin/bash

# Load conda and activate the environment
source /home/hamza/miniconda3/etc/profile.d/conda.sh
conda activate airflow_env

# Set Python interpreter for PySpark
export PYSPARK_PYTHON=/home/hamza/miniconda3/envs/airflow_env/bin/python
export PYSPARK_DRIVER_PYTHON=/home/hamza/miniconda3/envs/airflow_env/bin/python

# Run Spark job
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  --jars /home/hamza/Downloads/mysql-connector-j-8.0.33.jar \
  /home/hamza/DE/20212619_DS463_final/financial-transactions-project/spark/consumer_mysql.py
