import psycopg2

from config.config import DB_SETTINGS as db

def get_db_connection():
    conn = psycopg2.connect(
        host=db['HOST'],
        port=db['PORT'],
        dbname=db['NAME'],
        user=db['USER']
    )
    return conn