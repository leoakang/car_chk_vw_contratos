from dotenv import load_dotenv
import os
import pyodbc

load_dotenv(".env")

def createConnection(driver, server, database, username, password):
    connectionString = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}'
    
    try:
        conn = pyodbc.connect(connectionString)
        cursor = conn.cursor()
        return conn, cursor
    except pyodbc.Error as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        return None, None

def dataLakeDatabaseConnection():
    driver = os.getenv("ODBC_DRIVER")
    server = os.getenv("SERVER_DL")
    database = os.getenv("DATABASE_DL")
    username = os.getenv("USERNAME_DL")
    password = os.getenv("PASSWORD_DL")
    return createConnection(driver, server, database, username, password)

def databaseConnection():
    driver = os.getenv("ODBC_DRIVER")
    server = os.getenv("SERVER")
    database = os.getenv("DATABASE")
    username = os.getenv("USERNAME")
    password = os.getenv("PASSWORD")
    return createConnection(driver, server, database, username, password)

def closeConnection(conn, cursor):
    if cursor:
        cursor.close()
    if conn:
        conn.close()