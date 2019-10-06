import ndjson
import requests
import common
import psycopg2
import psycopg2.extras


def populate_tables():

    try:
        conn = common.get_db_connection()
        cur = conn.cursor()

        populate_patient_table(cur)

        cur.execute("SELECT COUNT(*) FROM encounter;")
        count_records = cur.fetchone()
        print(f'\n* There are {count_records[0]} in patient table')

        cur.close()
        conn.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            cur.close()
            conn.close()



def populate_patient_table(cur):

    patients_link = 'https://raw.githubusercontent.com/smart-on-fhir/flat-fhir-files/master/r3/Patient.ndjson'
    patients_data = get_ndjson(patients_link)

    for patient_data in patients_data:
        try:
            patient_data['country'] = patient_data['address'][0]['country']
        except:
            patient_data['country'] = None

        try:
            patient_data['race_code'] = patient_data['extension'][0]['valueCodeableConcept']['coding'][0]['code']
        except:
            patient_data['race_code'] = None

        try:
            patient_data['race_code_system'] = patient_data['extension'][0]['valueCodeableConcept']['coding'][0]['system']
        except:
            patient_data['race_code_system'] = None

        try:
            patient_data['ethnicity_code'] = patient_data['extension'][1]['valueCodeableConcept']['coding'][0]['code']
        except:
            patient_data['ethnicity_code'] = None

        try:
            patient_data['ethnicity_code_system'] = patient_data['extension'][1]['valueCodeableConcept']['coding'][0]['system']
        except:
            patient_data['ethnicity_code_system'] = None


        cur.execute("""
            INSERT INTO patient (
                source_id,
                gender,
                birth_date,
                country,
                race_code,
                race_code_system,
                ethnicity_code,
                ethnicity_code_system
            )
            VALUES (
                %(id)s,
                %(gender)s,
                %(birthDate)s,
                %(country)s,
                %(race_code)s,
                %(race_code_system)s,
                %(ethnicity_code)s,
                %(ethnicity_code_system)s
            )
            """, patient_data)


def get_ndjson(link):
    response = requests.get(link)
    return response.json(cls=ndjson.Decoder)


if __name__ == '__main__':
    populate_tables()
