import os
import json
import boto3
from dotenv import load_dotenv
from pymongo import MongoClient, errors


def load_environment_variables():
    load_dotenv()
    return {
        "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
        "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
        "db_host": os.getenv("DocumentDB_HOST"),
        "db_port": os.getenv("DocumentDB_PORT"),
        "db_user": os.getenv("DocumentDB_USER"),
        "db_password": os.getenv("DocumentDB_PASSWORD"),
    }


def create_s3_session(credentials):
    session = boto3.Session(
        aws_access_key_id=credentials["aws_access_key_id"],
        aws_secret_access_key=credentials["aws_secret_access_key"],
    )
    return session.resource("s3")


def fetch_trades_json(s3):
    bucket = s3.Bucket("stori-challenge-jd")
    objs = list(bucket.objects.filter(Prefix="trades.json"))

    if not objs or objs[0].key != "trades.json":
        print("trades.json file not found in the stori-challenge bucket")
        exit()

    content_object = s3.Object("stori-challenge-jd", "trades.json")
    file_content = content_object.get()["Body"].read().decode("utf-8")
    return json.loads(file_content)


def connect_to_db(credentials):
    return MongoClient(
        host=credentials["db_host"],
        port=int(credentials["db_port"]),
        username=credentials["db_user"],
        password=credentials["db_password"],
        authSource="admin",
        retryWrites=False,
    )["stori-docdb-2023"]


def get_collection_validator(db, collection_name):
    """Obtiene el validador para una colección específica."""
    collection_info = db.command("listCollections", filter={"name": collection_name})
    for info in collection_info.get("cursor", {}).get("firstBatch", []):
        if (
            info["name"] == collection_name
            and "options" in info
            and "validator" in info["options"]
        ):
            return info["options"]["validator"]
    return None


def update_or_insert_trades(db, trades_data, validate_schema=True):
    if validate_schema:
        collection_validator = get_collection_validator(db, "trades")
        if not collection_validator:
            print("No se pudo obtener el validador para la colección 'trades'.")
            return

        required_fields = set(collection_validator["$jsonSchema"]["required"])
    else:
        required_fields = set()

    for doc in trades_data:
        if validate_schema:
            doc_keys = set(doc.keys())

            # Comprueba si todas las claves requeridas están presentes en el documento
            if not required_fields.issubset(doc_keys):
                missing_keys = required_fields - doc_keys
                print(
                    f"El documento con id {doc['id']} le faltan las claves: {', '.join(missing_keys)}"
                )
                continue

        try:
            # Actualiza o inserta el documento en la colección
            db.trades.update_one({"id": doc["id"]}, {"$set": doc}, upsert=True, bypass_document_validation=not(validate_schema))
            # imprime el resultado de la operación
            print(f"Documento {doc['id']} insertado/actualizado correctamente.")
        except errors.WriteError as e:
            print(f"Falla al insertar documento {doc['id']}. Documento: {doc}. Error: {e.details}")



def main():
    credentials = load_environment_variables()
    s3 = create_s3_session(credentials)
    trades_data = fetch_trades_json(s3)
    db = connect_to_db(credentials)
    update_or_insert_trades(
        db, trades_data["data"], validate_schema=False
    )  # Set to False to disable schema validation


if __name__ == "__main__":
    main()
