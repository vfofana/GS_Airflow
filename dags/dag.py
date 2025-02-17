from airflow.decorators import task
from airflow.models.dag import dag
from airflow.operators.empty import EmptyOperator
import requests
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

DATA_FILE_NAME = "dags/data/data.json"

@task
def get_flight_data(column, url, creds, data_file_name):
    req = requests.get(url, auth=HTTPBasicAuth(creds["username"], creds["password"]))
    req.raise_for_status()
    resp = req.json()
    timestamp = resp['time']
    states_list = resp['states']
    states_json = [dict(zip(column,state)) for state in states_list]
    with open(data_file_name, 'w') as f:
        json.dump(states_json,f)

@task
def load_from_file(data_file_name):
    conn = None
    try:
        conn = duckdb.connect('dags/data/bdd_airflow')
        conn.sql(f"INSERT INTO bdd_airflow.main.openskynetwork_brute (SELECT * FROM'{data_file_name}')")

    finally:
        if conn:
            conn.close()


@task
def check_row_number():
    conn = None
    nbre_lignes = 0
    try:
        conn = duckdb.connect("dags/data/bdd_airflow", read_only=True)
        nbre_lignes = conn.sql(f"SELECT COUNT(*) FROM bdd_airflow.main.openskynetwork_brute").fetchone()[0]
    finally:
        if conn:
            conn.close()

    print(f"Le nombre de lignes: {nbre_lignes}")

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
        >> get_flight_data(COLONNES_OPEN_SKY,URL_ALL_STATES,CREDENTIALS, DATA_FILE_NAME)
        >> load_from_file(DATA_FILE_NAME)
        >> [check_row_number(),check_duplicates()]
        >>EmptyOperator(task_id="end")
    )


flights_pipeline_dag = flights_pipeline()