import os
import pandas as pd
import boto3
import json
from dotenv import load_dotenv
import psycopg2
from pymongo import MongoClient
from sqlalchemy import create_engine, inspect
from urllib.parse import quote_plus


def obtener_endpoint_redshift(client, cluster_name="stori-dw"):
    """
    Busca el cluster de Redshift con el nombre proporcionado y devuelve su endpoint.

    Parámetros:
    - cluster_name (str): El nombre del cluster de Redshift.

    Devoluciones:
    - str: El endpoint del cluster o None si no se encuentra el cluster.
    """

    try:
        # Obtiene los detalles del cluster
        response = client.describe_clusters(ClusterIdentifier=cluster_name)

        # Verifica si el cluster está disponible
        cluster = response["Clusters"][0]
        if cluster["ClusterStatus"] == "available":
            return cluster["Endpoint"]["Address"]
        else:
            print(f"Cluster {cluster_name} no está disponible.")
            return None
    except Exception as e:
        print(f"Error al obtener el endpoint para el cluster {cluster_name}: {e}")
        return None


def cargar_variables_ambiente():
    load_dotenv()
    return {
        "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
        "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
        "db_host": os.getenv("DB_HOST"),
        "db_port": int(os.getenv("DB_PORT")),
        "db_user": os.getenv("DB_USER"),
        "db_password": os.getenv("DB_PASSWORD"),
        "rs_user": os.getenv("rsDB_USER"),
        "rs_password": os.getenv("rsDB_PASSWORD"),
        "rs_region": os.getenv("rsDB_REGION"),
        "docdb_host": os.getenv("DocumentDB_HOST"),
        "docdb_user": os.getenv("DocumentDB_USER"),
        "docdb_password": os.getenv("DocumentDB_PASSWORD"),
        "docdb_port": int(os.getenv("DocumentDB_PORT")),
    }


def crear_cliente_redshift(credentials):
    session = boto3.Session(
        aws_access_key_id=credentials["aws_access_key_id"],
        aws_secret_access_key=credentials["aws_secret_access_key"],
        region_name=credentials["rs_region"],
    )
    return session.client("redshift")


def verificar_cluster_redshift(client, credentials):
    try:
        client.describe_clusters(ClusterIdentifier="stori-dw")
    except client.exceptions.ClusterNotFoundFault:
        client.create_cluster(
            DBName="stori-dw",
            ClusterIdentifier="stori-dw",
            ClusterType="single-node",
            NodeType="dc2.large",
            MasterUsername=credentials["rs_user"],
            MasterUserPassword=credentials["rs_password"],
        )


def conectar_postgresql(host, dbname, credentials):
    return psycopg2.connect(
        host=host,
        port=credentials["db_port"],
        dbname=dbname,
        user=credentials["db_user"],
        password=credentials["db_password"],
    )


def obtener_datos_postgresql(conn, query):
    return pd.read_sql(query, conn)


def cargar_datos_redshift(df, nombre_tabla, engine):
    df.to_sql(nombre_tabla, engine, if_exists="replace", method="multi", index=False)


def obtener_tablas_redshift(engine):
    inspector = inspect(engine)
    return inspector.get_table_names()


def obtener_datos_mongo(credentials):
    # Crear cadena de conexión URI
    user = quote_plus(credentials["docdb_user"])
    password = quote_plus(credentials["docdb_password"])
    connection_uri = f"mongodb://{user}:{password}@{credentials['docdb_host']}:{credentials['docdb_port']}"
    conn = MongoClient(connection_uri)
    db = conn["stori-docdb-2023"]
    table = pd.DataFrame(list(db.trades.find()))
    return table


def aplanar_datos_mongo(df):
    # Convertir el ObjectId a string
    df["_id"] = df["_id"].astype(str)

    # Aplanar el campo 'details'
    df["asks"] = df["details"].apply(lambda x: x["asks"] if "asks" in x else None)
    df["bids"] = df["details"].apply(lambda x: x["bids"] if "bids" in x else None)
    df["lag"] = df["details"].apply(lambda x: x["lag"] if "lag" in x else None)
    df["system"] = df["details"].apply(lambda x: x["system"] if "system" in x else None)

    # Aplanar el campo 'time'
    df["time_date"] = df["time"].apply(lambda x: x["date"] if "date" in x else None)

    # Aplana los arrays de 'asks' y 'bids'
    df["asks"] = df["asks"].apply(json.dumps)
    df["bids"] = df["bids"].apply(json.dumps)

    # Eliminar las columnas originales anidadas
    df.drop(columns=["details", "time"], inplace=True)

    return df


def main():
    credenciales = cargar_variables_ambiente()

    # Verificar y gestionar el cluster de Redshift
    cliente_redshift = crear_cliente_redshift(credenciales)
    verificar_cluster_redshift(cliente_redshift, credenciales)

    # Obtener datos de PostgreSQL
    conn_pg = conectar_postgresql(credenciales["db_host"], "stori", credenciales)
    df_transactions = obtener_datos_postgresql(conn_pg, "SELECT * FROM transactions")
    conn_pg.close()

    # Obtener el endpoint de Redshift
    endpoint = obtener_endpoint_redshift(cliente_redshift)
    if endpoint:
        print(f"Endpoint del cluster stori-dw: {endpoint}")
    else:
        print("No se pudo obtener el endpoint del cluster stori-dw.")

    # Cargar datos en Redshift
    engine_redshift = create_engine(
        f'redshift+psycopg2://{credenciales["rs_user"]}:{credenciales["rs_password"]}@{endpoint}:5439/stori-dw'
    )

    cargar_datos_redshift(df_transactions, "transactions", engine_redshift)

    # Obtener datos de MongoDB
    df_trades = obtener_datos_mongo(credenciales)

    # Aplanar datos de MongoDB
    df_trades = aplanar_datos_mongo(df_trades)

    # Cargar datos de trades en Redshift
    cargar_datos_redshift(df_trades, "trades", engine_redshift)

    # Listar las tablas en Redshift
    tablas = obtener_tablas_redshift(engine_redshift)
    print("Tablas en Redshift:", tablas)


if __name__ == "__main__":
    main()
