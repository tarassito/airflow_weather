import json
from collections import namedtuple
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.sqlite.operators.sqlite import SqliteOperator

City = namedtuple("City", ("name", "lat", "lon"))

Lviv = City("Lviv", "49.842957", "24.031111")
Kyiv = City("Kyiv", "50.450001", "30.523333")
Kharkiv = City("Kharkiv", "49.988358", "36.232845")
Odesa = City("Odesa", "46.482952", "30.712481")
Zhmerynka = City("Zhmerynka", "49.03705", "28.11201")


def process_weather(city, ti):
    info = ti.xcom_pull("extract_data")

    return {
        "city": city.name,
        "timestamp": info["data"][0]["dt"],
        "temp": info["data"][0]["temp"],
        "clouds": info["data"][0]["clouds"],
        "humidity": info["data"][0]["humidity"],
        "wind_speed": info["data"][0]["wind_speed"]
    }


for city in [Lviv, Kyiv, Kharkiv, Odesa, Zhmerynka]:

    request_params = {"appid": Variable.get("WEATHER_API_KEY"),
                      "lat": city.lat,
                      "lon": city.lon,
                      "dt": "{{ execution_date.add(hours=12).int_timestamp }}",
                      "units": "metric"}

    with DAG(dag_id=f"weather_dag_{city.name}",
             schedule_interval="@daily",
             start_date=datetime(2023, 3, 25, 13, 0, 0)) as dag:

        db_create = SqliteOperator(
            task_id="create_table_sqlite",
            sqlite_conn_id="db_conn",
            sql="""CREATE TABLE IF NOT EXISTS 
                    measures(
                        city TEXT,
                        timestamp TIMESTAMP,
                        temp FLOAT,
                        clouds INTEGER,
                        humidity INTEGER,
                        wind_speed FLOAT 
                    );"""
        )
        check_api = HttpSensor(
            task_id="check_api",
            http_conn_id="weather_api_conn",
            endpoint="data/3.0/onecall/timemachine?",
            request_params=request_params)

        extract_data = SimpleHttpOperator(
            task_id="extract_data",
            http_conn_id="weather_api_conn",
            endpoint="data/3.0/onecall/timemachine?",
            data=request_params,
            method="GET",
            response_filter=lambda x: json.loads(x.text),
            log_response=True
        )

        process_data = PythonOperator(
            task_id="process_data",
            python_callable=process_weather,
            op_args=[city]
        )

        inject_data = SqliteOperator(
            task_id="inject_data",
            sqlite_conn_id="db_conn",
            sql="""
            INSERT INTO measures(city, timestamp, temp, clouds, humidity, wind_speed) VALUES 
            ('{{ti.xcom_pull(task_ids='process_data')['city']}}',
            {{ti.xcom_pull(task_ids='process_data')['timestamp']}},
            {{ti.xcom_pull(task_ids='process_data')['temp']}},
            {{ti.xcom_pull(task_ids='process_data')['clouds']}},
            {{ti.xcom_pull(task_ids='process_data')['humidity']}},
            {{ti.xcom_pull(task_ids='process_data')['wind_speed']}}
            );"""
        )

        db_create >> check_api >> extract_data >> process_data >> inject_data
