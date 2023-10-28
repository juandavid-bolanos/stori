import os
import pandas as pd
import boto3
from dotenv import load_dotenv
import psycopg2


def load_config():
    load_dotenv()
    return {
        "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
        "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
        "db_host": os.getenv("DB_HOST"),
        "db_port": os.getenv("DB_PORT"),
        "db_user": os.getenv("DB_USER"),
        "db_password": os.getenv("DB_PASSWORD"),
    }


def get_s3_dataframe(config):
    s3 = boto3.resource(
        "s3",
        aws_access_key_id=config["aws_access_key_id"],
        aws_secret_access_key=config["aws_secret_access_key"],
        region_name="sa-east-1",
    )
    bucket = s3.Bucket("stori-challenge-jd")
    objs = list(bucket.objects.filter(Prefix="txns.csv"))

    if not (len(objs) > 0 and objs[0].key == "txns.csv"):
        print("txns.csv file not found in the stori-challenge bucket")
        exit()

    boto3_client = boto3.client(
        "s3",
        aws_access_key_id=config["aws_access_key_id"],
        aws_secret_access_key=config["aws_secret_access_key"],
        region_name="sa-east-1",
    )
    response = boto3_client.get_object(Bucket="stori-challenge-jd", Key="txns.csv")

    df = pd.read_csv(response["Body"])
    df.columns = df.columns.str.strip().str.upper()
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    # Column casting
    df["DATE"] = pd.to_datetime(df["DATE"], format="%d-%b-%y")
    df["VALUE_DATE"] = pd.to_datetime(df["VALUE_DATE"], format="%d-%b-%y")
    for col in ["WITHDRAWAL_AMT", "DEPOSIT_AMT", "BALANCE_AMT"]:
        df[col] = df[col].str.replace(",", "").astype(float)
    df["ACCOUNT_NO"] = df["ACCOUNT_NO"].astype(int)

    return df


def create_db_connection(config):
    return psycopg2.connect(
        host=config["db_host"],
        port=config["db_port"],
        dbname="stori",
        user=config["db_user"],
        password=config["db_password"],
    )


def get_table_schema(cur):
    cur.execute(
        """
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = 'transactions'
    """
    )
    return sorted(
        [column[0].upper() for column in cur.fetchall() if column[0].upper() != "ID"]
    )


def upload_dataframe_to_db(df, conn):
    cur = conn.cursor()
    table_schema = get_table_schema(cur)
    file_schema = sorted(df.columns)

    if table_schema != file_schema:
        print(
            "The schema of the CSV file does not match the schema of the transactions table"
        )
        exit()

    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ",".join(list(df.columns))
    query = (
        "INSERT INTO transactions ({}) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)".format(
            cols
        )
    )
    cur.executemany(query, tuples)
    conn.commit()
    cur.close()


def main():
    config = load_config()
    df = get_s3_dataframe(config)
    conn = create_db_connection(config)
    upload_dataframe_to_db(df, conn)
    conn.close()


if __name__ == "__main__":
    main()
