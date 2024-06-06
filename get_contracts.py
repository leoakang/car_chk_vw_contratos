from dotenv import load_dotenv
from datetime import datetime
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from db_connection import dataLakeDatabaseConnection, databaseConnection, closeConnection

def main ():
    load_dotenv(".env")
    pd.set_option('future.no_silent_downcasting', True)
    timeStart = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    try:
        conn, cursor = databaseConnection()
        dataSet = cursor.execute("SELECT * FROM chk.viewExportContratos").fetchall()
        columns = [column[0] for column in cursor.description]
        df = pd.DataFrame.from_records(dataSet, columns=columns)
        df.replace('', np.nan, inplace=True)
        df.insert(0, "DATA_REF", timeStart)

        try:
            conn,cursor = dataLakeDatabaseConnection()
            engine = create_engine("mssql+pyodbc://", creator=lambda: conn)
            cursor.execute("TRUNCATE TABLE chk_viewExportContratos").commit()
            df.to_sql("chk_viewExportContratos", engine, if_exists = "append", index = False)
            print("Dados inseridos com sucesso na tabela chk_viewExportContratos")
                        
        except Exception as e:
            print(f"Erro ao inserir dados na tabela chk_viewExportContratos: {e}")

    except Exception as e:
        print(f"Erro ao coletar os contratos: {e}")

    finally:
        closeConnection(conn,cursor)


if __name__ == "__main__":
    main()