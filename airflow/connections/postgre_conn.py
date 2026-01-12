import os
import psycopg

def get_postgre_conn():
    return psycopg.connect(
            dbname=os.getenv("DJANGO_DB"),
            user=os.getenv("DJANGO_POSTGRES_USER"),
            password=os.getenv("DJANGO_POSTGRES_PASSWORD"),
            host=os.getenv("POSTGRES_HOST"),
            port=int(os.getenv("POSTGRES_PORT")),
        )
