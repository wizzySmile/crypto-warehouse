#!/bin/bash
airflow db migrate
airflow scheduler &
exec airflow webserver --host 0.0.0.0 --port 8080