from airflow.decorators import task
from airflow.models.dag import dag
from airflow.operators.empty import EmptyOperator
import requests
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from requests.auth import HTTPBasicAuth
import duckdb
import json

COLONNES_OPEN_SKY = [
    "icao24",
    "callsign",
    "origin_country",
    "time_position",
    "last_contact",
    "longitude",
    "latitude",
    "baro_altitude",
    "on_ground",
    "velocity",
    "true_track",
    "vertical_rate",
    "sensors",
    "geo_altitude",
    "squawk",
    "spi",
    "position_source",
    "category"
]

URL_ALL_STATES = "https://opensky-network.org/api/states/all?extended=true"

CREDENTIALS = {"username": 'FOFANA',
               "password": "Juni0r2104."}

def to_dict(states_list, colonnes, timestamp):
    out = []
    for state in states_list:
        states_dict = dict(zip(colonnes, state))
        states_dict['timestamp'] = timestamp
        out.append(states_dict)
    return out

@task(multiple_outputs=True)
def get_flight_data(column, url, creds):
    req = requests.get(url, auth=HTTPBasicAuth(creds["username"], creds["password"]))
    req.raise_for_status()
    resp = req.json()
    timestamp = resp['time']
    states_list = resp['states']

    states_json = to_dict(states_list,column,timestamp)

    data_file_name = f"dags/data/data_{timestamp}.json"
    with open(data_file_name, 'w') as f:
        json.dump(states_json,f)

    return {
        'filename' : data_file_name,
        'timestamp' : timestamp,
        'rows' : len(states_json)
    }


def load_from_file():
    return SQLExecuteQueryOperator(
        task_id="load_from_file",
        conn_id="DUCK_DB",
        sql="INSERT INTO bdd_airflow.main.openskynetwork_brute (SELECT * FROM'{{ti.xcom_pull(task_ids='get_flight_data', key='filename')}}')",
        return_last=True,
        show_return_value_in_logs=True
    )


@task
def check_row_number(ti=None):

    nbre_lignes_prevues =  ti.xcom_pull(task_ids="get_flight_data", key="rows")
    nbre_lignes_attendues = ti.xcom_pull(task_ids="load_from_file", key="return_value")[0][0]
    if nbre_lignes_attendues != nbre_lignes_prevues:
        raise Exception(f"Nombre de lignes dans la base ({nbre_lignes_attendues}) != nombre de lignes prevues de l'API ({nbre_lignes_prevues})")
    print(f"Le nombre de lignes: {nbre_lignes_attendues}")


def check_duplicates():
    return SQLExecuteQueryOperator(
        task_id="check_duplicates",
        conn_id="DUCK_DB",
        sql="check_duplicates.sql",
        return_last=True,
        show_return_value_in_logs=True
    )

@dag()
def flights_pipeline():
    (
        EmptyOperator(task_id="start")
        >> get_flight_data(COLONNES_OPEN_SKY,URL_ALL_STATES,CREDENTIALS)
        >> load_from_file()
        >> [check_row_number(),check_duplicates()]
        >>EmptyOperator(task_id="end")
    )


flights_pipeline_dag = flights_pipeline()