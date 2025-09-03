from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "customer_events_dag",
    default_args=default_args,
    description="Run Spark job for customer_events_write",
    schedule="@once",  # run once when scheduler starts
    start_date=datetime(2025, 8, 24),
    catchup=False,
    tags=["spark", "customer"],
) as dag:

    customer_events_task = SparkSubmitOperator(
        task_id="customer_events_write",
        application="/opt/spark/jobs/customer_events_write.py",  # inside your container
        conn_id="spark_default",
        name="customer_events_write_job",
        execution_timeout=timedelta(hours=1),
        conf={
        "spark.master": "spark://spark-master:7077"  # service name in docker-compose
    }
    )
