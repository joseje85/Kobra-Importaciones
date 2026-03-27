import pandas as pd
import pyodbc
import logging
import psycopg2
import time
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

# Cargar .env global
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))


def connect_to_database_sqlserver(database, max_retries=10, delay=1):
    """
    Conecta a SQL Server con reintentos y retorna la conexión pyodbc.
    """
    retries = 0
    conn = None

    # Parámetros de conexión
    server = os.getenv("SQL_HOST")
    database = database       
    user = os.getenv("SQL_USER")  
    password = os.getenv("SQL_PASSWORD")            

    # Cadena de conexión ODBC
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={user};"
        f"PWD={password}"
    )

    while retries < max_retries:
        try:
            conn = pyodbc.connect(conn_str)
            print("Conexión exitosa a SQL Server.")
            break
        except pyodbc.Error as e:
            retries += 1
            print(f"Intento {retries} de {max_retries} fallido: {e}")
            if retries < max_retries:
                print(f"Reintentando en {delay} segundos...")
                time.sleep(delay)
            else:
                print("Se alcanzó el número máximo de reintentos. No se pudo conectar a SQL Server.")
    return conn

