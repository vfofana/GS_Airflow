import time
from datetime import datetime

import duckdb
from airflow.decorators import task, task_group
from airflow.models import Param
from airflow.models.dag import dag
from airflow.operators.empty import EmptyOperator
import requests
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sensors.filesystem import FileSensor
from marshmallow.utils import timestamp
from requests.auth import HTTPBasicAuth
import json

from sqlalchemy.sql import True_

sql_path = "dags/sql/"

CREDENTIALS = {"username": 'FOFANA',
               "password": "Juni0r2104."}

liste_des_apis = [
    {
    "schedule":"30 9 * * *",
    "nom":"states",
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
    {
    "schedule":"30 13 * * *",
    "nom":"flights",
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

]



def format_datetime(input_datetime):
    return input_datetime.strftime("%Y%m%d")

@task(multiple_outputs=True)
def run_parameters(api, dag_run=None, ti=None):

    out = api

    date_interval_start = format_datetime(dag_run.data_interval_start)
    date_interval_end = format_datetime(dag_run.data_interval_end)

    data_file_name = f"dags/data/data_{out['nom']}_{date_interval_start}_{date_interval_end}.json"
    out["data_file_name"] = data_file_name

    #SQL pour charger les données dans le DWH
    with open(sql_path+"load_from_file.sql","r") as f:
        load_from_file_sql = f.read().format(target_table=out["target_table"], data_file_name = out["data_file_name"])
    out["load_from_file_sql"] = load_from_file_sql

    #SQL pour compter les lignes dans le DWH
    with open(sql_path+"check_db_row.sql", "r") as f:
        check_db_row_sql = f.read()
    out["check_db_row_sql"] = check_db_row_sql

    #SQL pour verifier le nombre de lignes dupliquées dans le DWH
    with open(sql_path+"check_duplicates.sql", "r") as f:
        check_duplicates_sql = f.read().format(target_table=out["target_table"])
    out["check_duplicates_sql"] = check_duplicates_sql

    if out["timestamp_required"]:
        end = int(time.time())
        begin = end-3600
        out["url"] = out["url"].format(begin=begin,end=end)

    return {"run_params":out}

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

@task_group
def ingestion_data_tg(creds, run_params):
    get_flight_data(creds=creds, run_params=run_params) >> load_from_file(run_params)

@task_group
def data_quality_tg(run_params):
    check_row_number(run_params)
    check_duplicates(run_params)

@task(multiple_outputs=True)
def get_flight_data(creds,run_params):

    # Retrouve les paramètres de XCOM
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

@task
def load_from_file(run_params):
    with duckdb.connect("dags/data/bdd_airflow") as conn:
        conn.sql(run_params['load_from_file_sql'])


@task
def check_row_number(run_params, ti=None):
    # Récupérer les résultats de la tâche get_flight_data et eviter plusieurs appels.
    get_flight_data = ti.xcom_pull(task_ids="ingestion_data_tg.get_flight_data", key="return_value")

    with duckdb.connect("dags/data/bdd_airflow") as conn:
        if not run_params["timestamp_required"]:
            timestamp = get_flight_data[0]['timestamp']
            rows_states = get_flight_data[0]['rows']
            rows_states_except = conn.sql(run_params['check_db_row_sql'].format(target_table=run_params["target_table"],timestamp=timestamp)).fetchone()[0]
            print(f"Nombre de lignes prévues: {rows_states} contre nombre de lignes trouvées: {rows_states_except}")
            if rows_states != rows_states_except:
                raise Exception(
                    f"Nombre de lignes dans le DWH ({rows_states_except}) != nombre de lignes prévues de l'API ({rows_states})"
                )
            else:
                print("States data OK")
        else:
            timestamp = get_flight_data[1]['timestamp']
            rows_flights = get_flight_data[1]['rows']
            rows_flight_except = conn.sql(run_params['check_db_row_sql'].format(target_table=run_params["target_table"], timestamp=timestamp)).fetchone()[0]
            print(f"Nombre de lignes prévues: {rows_flights} contre nombre de lignes trouvées: {rows_flight_except}")
            if rows_flights != rows_flight_except:
                raise Exception(
                    f"Nombre de lignes dans le DWH ({rows_flight_except}) != nombre de lignes prévues de l'API ({rows_flights})"
                )
            else:
                print("Flights data OK")

@task
def check_duplicates(run_params):
    with duckdb.connect("dags/data/bdd_airflow") as conn:
        nb_lignes_duplicates= conn.sql(run_params['check_duplicates_sql']).fetchone()[0]

    print(f"Lignes dupliquées={nb_lignes_duplicates}")

@dag(
        start_date=datetime(2025, 2, 1),
        schedule=None,
        catchup=False,
        concurrency=1,
)
def flights_pipeline():
    run_parameters_task = run_parameters.expand(api=liste_des_apis)
    (
            EmptyOperator(task_id="start")
            >> run_parameters_task
            >> ingestion_data_tg.partial(creds=CREDENTIALS).expand_kwargs(run_parameters_task)
            >> data_quality_tg.expand_kwargs(run_parameters_task)
            >> EmptyOperator(task_id="end")
    )


flights_pipeline()