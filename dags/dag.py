from airflow.decorators import task
from airflow.models.dag import dag
from airflow.operators.empty import EmptyOperator
import requests
from marshmallow.utils import timestamp
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

@task
def load_from_file(ti=None):
    conn = None
    data_file_name = ti.xcom_pull(task_ids="get_flight_data", key="filename")
    try:
        conn = duckdb.connect('dags/data/bdd_airflow')
        conn.sql(f"INSERT INTO bdd_airflow.main.openskynetwork_brute (SELECT * FROM'{data_file_name}')")

    finally:
        if conn:
            conn.close()


@task
def check_row_number(ti=None):
    conn = None
    nbre_lignes_attendues = 0
    print(f"timestamp = {ti.xcom_pull(task_ids='get_flight_data', key='timestamp')}")
    contenu_xcom =  ti.xcom_pull(task_ids="get_flight_data", key="return_value")
    timestamp = contenu_xcom["timestamp"]
    nbre_lignes_prevues = contenu_xcom["rows"]
    try:
        conn = duckdb.connect("dags/data/bdd_airflow", read_only=True)
        nbre_lignes_attendues = conn.sql(f"SELECT COUNT(*) FROM bdd_airflow.main.openskynetwork_brute WHERE timestamp = {timestamp}").fetchone()[0]
    finally:
        if conn:
            conn.close()
    if nbre_lignes_attendues != nbre_lignes_prevues:
        raise Exception(f"Nombre de lignes dans la base ({nbre_lignes_attendues}) != nombre de lignes prevues de l'API ({nbre_lignes_prevues})")
    print(f"Le nombre de lignes: {nbre_lignes_attendues}")

@task
def check_duplicates():
    conn = None
    nbre_duplicates = 0
    try:
        conn = duckdb.connect("dags/data/bdd_airflow", read_only=True)
        nbre_duplicates = conn.sql("""
        SELECT callsign, time_position, last_contact, count(*) as cnt
        FROM bdd_airflow.main.openskynetwork_brute
        GROUP BY 1,2,3
        HAVING cnt > 1
        """).count(column='cnt').fetchone()[0]
    finally:
        if conn:
            conn.close()

    print(f"Le nombre de lignes dupliquées: {nbre_duplicates}")

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