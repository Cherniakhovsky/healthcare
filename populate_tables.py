import ndjson
import requests
import common
import psycopg2


def populate_tables():

    patients_link = 'https://raw.githubusercontent.com/smart-on-fhir/flat-fhir-files/master/r3/Patient.ndjson'

    patients_data = get_ndjson(patients_link)

    try:
        conn = common.get_db_connection()
        cur = conn.cursor()

        populate_patient_table(cur, patients_data)

        cur.close()
        conn.commit()


    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            cur.close()
            conn.close()


def populate_patient_table(cur, patients_data):
    cur.executemany("""
            INSERT INTO patient (
                source_id,
                gender,
                birth_date
            )
            VALUES (
                %(id)s,
                %(gender)s,
                %(birthDate)s 
            )""", patients_data[:3])


def get_ndjson(link):
    response = requests.get(link)
    return response.json(cls=ndjson.Decoder)


if __name__ == '__main__':
    populate_tables()
