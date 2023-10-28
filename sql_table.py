import os
from os.path import join, dirname
from dotenv import load_dotenv
import psycopg2
from psycopg2 import sql, extensions


def load_config():
    dotenv_path = join(dirname(__file__), '.env')
    load_dotenv(dotenv_path)
    
    return {
        'db_host': os.getenv('DB_HOST'),
        'db_port': os.getenv('DB_PORT'),
        'db_user': os.getenv('DB_USER'),
        'db_password': os.getenv('DB_PASSWORD')
    }


def create_server_connection(config):
    return psycopg2.connect(
        host=config['db_host'],
        port=config['db_port'],
        user=config['db_user'],
        password=config['db_password']
    )


def ensure_db_exists(conn):
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM pg_database WHERE datname='stori'")
    if not cur.fetchone():
        conn.commit()
        conn.autocommit = True
        cur.execute("CREATE DATABASE stori")
        conn.autocommit = False
        conn.set_isolation_level(extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cur.execute("COMMENT ON DATABASE stori IS 'Database for storing transaction and trading data'")
    cur.close()


def create_db_connection(config):
    return psycopg2.connect(
        host=config['db_host'],
        port=config['db_port'],
        dbname='stori',
        user=config['db_user'],
        password=config['db_password']
    )


def execute_ddl_from_file(conn, filename):
    with open(filename, 'r') as file:
        ddl = sql.SQL(file.read())
    cur = conn.cursor()
    cur.execute(ddl)
    cur.close()


def main():
    config = load_config()

    conn = create_server_connection(config)
    ensure_db_exists(conn)
    conn.close()

    conn = create_db_connection(config)
    execute_ddl_from_file(conn, 'transactions.sql')
    
    conn.commit()
    conn.close()


if __name__ == '__main__':
    main()
