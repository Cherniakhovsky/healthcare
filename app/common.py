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


def num_to_week_day(number):
    if number == 0:
        return 'Sunday'
    elif number == 1:
        return 'Monday'
    elif number == 2:
        return 'Tuesday'
    elif number == 3:
        return 'Wednesday'
    elif number == 4:
        return 'Thursday'
    elif number == 5:
        return 'Friday'
    elif number == 6:
        return 'Saturday'
