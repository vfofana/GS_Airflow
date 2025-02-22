import time
from datetime import datetime

from airflow.decorators import task
from airflow.models import Param
from airflow.models.dag import dag
from airflow.operators.empty import EmptyOperator
import requests
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sensors.filesystem import FileSensor
from requests.auth import HTTPBasicAuth
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

endpoint_to_params = {
    "states":{
        "url":"https://opensky-network.org/api/states/all?extended=true",
        "colonnes" : ["icao24","callsign",
                      "origin_country","time_position",
                      "last_contact","longitude",
                      "latitude","baro_altitude",
                      "on_ground","velocity",
                      "true_track","vertical_rate",
                      "sensors","geo_altitude",
                      "squawk","spi",
                      "position_source","category"
                      ],
        "target_table":"bdd_airflow.main.openskynetwork_brute",
        "timestamp_required" : False
            },
    "flights":{
        "url":"https://opensky-network.org/api/flights/all?begin={begin}&end={end}",
        "colonnes" : [
            "icao24","firstSeen",
            "estDepartureAirport","lastSeen",
            "estArrivalAirport","callsign",
            "estDepartureAirportHorizDistance","estDepartureAirportVertDistance",
            "estArrivalAirportHorizDistance","estArrivalAirportVertDistance",
            "departureAirportCandidatesCount","arrivalAirportCandidatesCount",
            "timestamp"
        ],
        "target_table":"bdd_airflow.main.flights_brute",
        "timestamp_required" : True
    }

}

def format_datetime(input_datetime):
    return input_datetime.strftime("%Y%m%d")

@task(multiple_outputs=True)
def run_parameters(params=None, dag_run=None):

    date_interval_start = format_datetime(dag_run.data_interval_start)
    date_interval_end = format_datetime(dag_run.data_interval_end)

    out = endpoint_to_params[params["endpoint"]]
    out ["data_file_name"] = f"dags/data/data_{date_interval_start}_{date_interval_end}.json"

    if out["timestamp_required"]:
        end = int(time.time())
        begin = end-3600
        out["url"] = out["url"].format(begin=begin,end=end)
    return out

def flights_to_dict(flights,timestamp):
    out = []

    for flight in flights:
        flight["timestamp"] = timestamp
        out.append(flight)

    return out

def states_to_dict(states_list, colonnes, timestamp):
    out = []
    for state in states_list:
        states_dict = dict(zip(colonnes, state))
        states_dict['timestamp'] = timestamp
        out.append(states_dict)
    return out

@task(multiple_outputs=True)
def get_flight_data(creds,ti=None):

    # Retrouve les paramètres de XCOM
    run_params = ti.xcom_pull(task_ids="run_parameters", key="return_value")
    data_file_name = run_params["data_file_name"]
    url = run_params["url"]
    column = run_params["colonnes"]

    # Telecharge les données
    req = requests.get(url, auth=HTTPBasicAuth(creds["username"], creds["password"]))
    req.raise_for_status()
    resp = req.json()

    #Transforme les données selon l'API d'origine
    if "states" in resp:
        timestamp = resp['time']
        response_json= states_to_dict(resp['states'],column,timestamp)
    else:
        timestamp = int(time.time())
        response_json = flights_to_dict(resp,timestamp)

    with open(data_file_name, 'w') as f:
        json.dump(response_json,f)

    return {
        'filename' : data_file_name,
        'timestamp' : timestamp,
        'rows' : len(response_json)
    }


def load_from_file():
    return SQLExecuteQueryOperator(
        task_id="load_from_file",
        conn_id="DUCK_DB",
        sql="load_from_file.sql",
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

@dag(
    params={"endpoint":Param(
        default="states",
        enum = list(endpoint_to_params.keys())
    )},
    start_date = datetime(2025,2,22),
    schedule="0 0 * * *",
    catchup=True,
    concurrency=1,
)
def flights_pipeline():
    (
        EmptyOperator(task_id="start")
        >>FileSensor(
        task_id="attendre_les_données",
        fs_conn_id="connection_fichier",
        filepath="dags/new_data/{{params.endpoint}}",
        poke_interval=120,
        mode="reschedule",
        timeout=600
        )
        >>run_parameters()
        >> get_flight_data(CREDENTIALS)
        >> load_from_file()
        >> [check_row_number(),check_duplicates()]
        >>EmptyOperator(task_id="end")
    )


flights_pipeline_dag = flights_pipeline()