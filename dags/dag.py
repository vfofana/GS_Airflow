from airflow.decorators import task
from airflow.models.dag import dag
from airflow.operators.empty import EmptyOperator
import requests
from requests.auth import HTTPBasicAuth

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

@task
def get_flight_data(column, url, creds):
    req = requests.get(url, auth=HTTPBasicAuth(creds["username"], creds["password"]))
    req.raise_for_status()
    resp = req.json()
    timestamp = resp['time']
    states_list = resp['states']
    states_json = [dict(zip(column,state)) for state in states_list]
    return {"timestamp":timestamp, "states_json":states_json}


@dag()
def flights_pipeline():
    (
        EmptyOperator(task_id="start")
        >> get_flight_data(COLONNES_OPEN_SKY,URL_ALL_STATES,CREDENTIALS)
        >>EmptyOperator(task_id="end")
    )


flights_pipeline_dag = flights_pipeline()