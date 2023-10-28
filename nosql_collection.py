import os
from dotenv import load_dotenv
from pymongo import MongoClient
import boto3


def load_config():
    load_dotenv()
    return {
        "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
        "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
        "aws_region": os.getenv("DocumentDB_REGION"),
        "db_host": os.getenv(
            "DocumentDB_HOST"
        ),  # Use localhost because you've created an SSH tunnel
        "db_port": "27017",  # Default MongoDB port
        "db_user": os.getenv("DocumentDB_USER"),
        "db_password": os.getenv("DocumentDB_PASSWORD"),
    }


def create_session(config):
    return boto3.Session(
        aws_access_key_id=config["aws_access_key_id"],
        aws_secret_access_key=config["aws_secret_access_key"],
        region_name=config["aws_region"],
    )


def ensure_db_exists(session, config):
    client = session.client("docdb")
    try:
        client.describe_db_instances(DBInstanceIdentifier="stori-docdb-2023")
    except client.exceptions.DBInstanceNotFoundFault:
        client.create_db_instance(
            DBInstanceIdentifier="stori-docdb-2023",
            DBInstanceClass="db.t3.medium",
            Engine="docdb",
            MasterUsername=config["db_user"],
            MasterUserPassword=config["db_password"],
        )
    return client


def create_collection(conn):
    db = conn["stori-docdb-2023"]

    schema = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": [
                "id",
                "details",
                "price",
                "shares",
                "ticker",
                "ticket",
                "time",
            ],
            "properties": {
                "id": {"bsonType": "string"},
                "details": {
                    "bsonType": "object",
                    "required": ["asks", "bids", "lag", "system"],
                    "properties": {
                        "asks": {"bsonType": "array"},
                        "bids": {"bsonType": "array"},
                        "lag": {"bsonType": "int"},
                        "system": {"bsonType": "string"},
                    },
                },
                "price": {"bsonType": "int"},
                "shares": {"bsonType": "int"},
                "ticker": {"bsonType": "string"},
                "ticket": {"bsonType": "string"},
                "time": {
                    "bsonType": "object",
                    "required": ["date"],
                    "properties": {"date": {"bsonType": "date"}},
                },
            },
        }
    }
    # db.create_collection('trades', validator=schema)
    db.create_collection("trades")


def main():
    config = load_config()
    session = create_session(config)
    ensure_db_exists(session, config)

    # Connect to the database. Since TLS is disabled, no need to set the SSL options
    conn = MongoClient(
        config["db_host"],
        int(config["db_port"]),
        username=config["db_user"],
        password=config["db_password"],
    )

    create_collection(conn)
    # Close the connection when done
    conn.close()


if __name__ == "__main__":
    main()
