# Import necessary libraries and modules
from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from datetime import datetime, timedelta
from src.lab import load_data, data_preprocessing, build_save_model, load_model_elbow
from airflow import configuration as conf

# Enable pickle support for XCom, allowing data to be passed between tasks
conf.set('core', 'enable_xcom_pickling', 'True')

# Define default arguments for your DAG
default_args = {
    'owner': 'your_name',
    'start_date': datetime(2025, 1, 15),
    'retries': 0,  # Number of retries in case of task failure
    'retry_delay': timedelta(minutes=5),  # Delay before retries
}

# Create a DAG instance named 'Airflow_Lab1' with the defined default arguments
dag = DAG(
    'Airflow_Lab1',
    default_args=default_args,
    description='DAG example for Lab 1 of Airflow series',
    schedule_interval=None,  # Set to None for manual triggering
    catchup=False,
)

# Define PythonOperators for each function

# Task to load data
load_data_task = PythonOperator(
    task_id='load_data_task',
    python_callable=load_data,
    dag=dag,
)

# Task to perform data preprocessing
data_preprocessing_task = PythonOperator(
    task_id='data_preprocessing_task',
    python_callable=data_preprocessing,
    op_args=[load_data_task.output],
    dag=dag,
)

# Task to build and save a model
build_save_model_task = PythonOperator(
    task_id='build_save_model_task',
    python_callable=build_save_model,
    op_args=[data_preprocessing_task.output, "model.sav"],
    dag=dag,
)

# --- Branching logic function ---
def branch_decision(**kwargs):
    # Example: in a real case, you'd read model metrics from XCom or a file
    accuracy = 0.82  # Simulated metric

    if accuracy >= 0.8:
        return 'load_model_task'  # Go to success path
    else:
        return 'retrain_model_task'  # Go to alternate path

# Branching task
branch_task = BranchPythonOperator(
    task_id='branch_decision_task',
    python_callable=branch_decision,
    dag=dag,
)

# Task to load model
load_model_task = PythonOperator(
    task_id='load_model_task',
    python_callable=load_model_elbow,
    op_args=["model.sav"],
    dag=dag,
)

# Task for retraining (alternate branch)
def retrain_model():
    print("Model accuracy too low — retraining or alerting triggered!")

retrain_model_task = PythonOperator(
    task_id='retrain_model_task',
    python_callable=retrain_model,
    dag=dag,
)

# Define task dependencies
load_data_task >> data_preprocessing_task >> build_save_model_task >> branch_task
branch_task >> [load_model_task, retrain_model_task]

# Optional CLI trigger
if __name__ == "__main__":
    dag.cli()