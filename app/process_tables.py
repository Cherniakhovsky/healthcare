import psycopg2
import operator

from app import common


def process_tables():
    """
    Retrieve data from tables
    """

    try:
        conn = common.get_db_connection()
        cur = conn.cursor()

        _count_records_in_every_table(cur)
        _get_number_patients_by_gender(cur)
        _get_top_10_procedure_types(cur)
        _get_most_and_least_popular_days(cur)

        cur.close()
        conn.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            cur.close()
            conn.close()


def _count_records_in_every_table(cur):
    print('\n1. Number of records imported into each table:')

    cur.execute("SELECT COUNT(*) FROM patient;")
    print(f'* in Patient table - {cur.fetchone()[0]} records')

    cur.execute("SELECT COUNT(*) FROM encounter;")
    print(f'* in Encounter table - {cur.fetchone()[0]} records')

    cur.execute("SELECT COUNT(*) FROM procedure;")
    print(f'* in Procedure table - {cur.fetchone()[0]} records')

    cur.execute("SELECT COUNT(*) FROM observation;")
    print(f'* in Observation table - {cur.fetchone()[0]} records')

def _get_number_patients_by_gender(cur):
    print('\n2. The number of patients by gender:')

    cur.execute("SELECT COUNT(*) FROM patient WHERE patient.gender = 'male';")
    print(f'* Male - {cur.fetchone()[0]}')

    cur.execute("SELECT COUNT(*) FROM patient WHERE patient.gender = 'female';")
    print(f'* Female - {cur.fetchone()[0]}')

def _get_top_10_procedure_types(cur):
    print('\n3. The top 10 types of procedures:')
    cur.execute("""SELECT type_code, COUNT(type_code) 
                   FROM procedure 
                   GROUP BY type_code 
                   ORDER BY COUNT(type_code) DESC
                   LIMIT 10;""")
    for record in cur:
        print(f'* {record[0]} type code is in {record[1]} rows')

def _get_most_and_least_popular_days(cur):
    print('\n4. The most and least popular days of the week when encounters occurred:')
    # cur.execute("select start_date from encounter limit 10")
    # cur.execute("select extract(dow from start_date)")
    cur.execute("""SELECT EXTRACT(DOW FROM start_date), COUNT(type_code) 
                   FROM encounter
                   GROUP BY EXTRACT(DOW FROM start_date)
                   ORDER BY COUNT(start_date) DESC;""")

    days = {}
    for record in cur:
        days[int(record[0])] = record[1]
    sorted_by_popularity = sorted(days.items(), key=operator.itemgetter(1))
    print('* The most popular is', common.num_to_week_day(sorted_by_popularity[-1][0]))
    print('* The least popular is', common.num_to_week_day(sorted_by_popularity[0][0]))


if __name__ == '__main__':
    process_tables()