from airflow.decorators import task
from airflow.models.dag import dag
from airflow.operators.empty import EmptyOperator

@task
def get_data():
    print("Telechagement des données")

@dag()
def flights_pipeline():
    (
        EmptyOperator(task_id="start")
        >> get_data()
        >>EmptyOperator(task_id="end")
    )


flights_pipeline_dag = flights_pipeline()