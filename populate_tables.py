import ndjson
import requests
import common
import psycopg2
import psycopg2.extras
import time


def populate_tables():
    populate_speed_in_sec = {'patient': 0, 'encounter': 0}
    try:
        conn = common.get_db_connection()
        cur = conn.cursor()

        populate_patient_table(cur, populate_speed_in_sec)

        populate_encounter_table(cur, populate_speed_in_sec)


        cur.execute("SELECT COUNT(*) FROM encounter;")
        count_records = cur.fetchone()
        print(f'\n* There are {count_records[0]} in encounter table')
        print(populate_speed_in_sec)

        cur.close()
        conn.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            cur.close()
            conn.close()


def populate_patient_table(cur, count_performance):
    """

    There are not required fields in try/except
    """

    patients_link = 'https://raw.githubusercontent.com/smart-on-fhir/flat-fhir-files/master/r3/Patient.ndjson'
    patients_data = get_ndjson(patients_link)


    start = time.time()
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

    count_performance['patient'] = round(time.time() - start, 2)


def populate_encounter_table(cur, count_performance):
    """

    There are not required fields in try/except
    """

    encounter_link = 'https://raw.githubusercontent.com/smart-on-fhir/flat-fhir-files/master/r3/Encounter.ndjson'
    encounters_data = get_ndjson(encounter_link)

    start = time.time()
    for encounter_data in encounters_data:

        ref_id = encounter_data['subject']['reference'].split('/')[-1]

        query = f"SELECT id FROM patient WHERE patient.source_id = '{ref_id}'"
        cur.execute(query)
        encounter_data['patient_id'] = cur.fetchone()[0]

        encounter_data['start_date'] = encounter_data['period']['start']
        encounter_data['end_date'] = encounter_data['period']['end']
        encounter_data['type_code'] = encounter_data['type'][0]['coding'][0]['code']
        encounter_data['type_code_system'] = encounter_data['type'][0]['coding'][0]['system']


        try:
            encounter_data['type_code'] = encounter_data['type'][0]['coding'][0]['code']
        except:
            encounter_data['type_code'] = None

        try:
            encounter_data['type_code_system'] = encounter_data['type'][0]['coding'][0]['system']
        except:
            encounter_data['type_code_system'] = None


        cur.execute("""
                INSERT INTO encounter (
                    source_id,
                    patient_id,
                    start_date,
                    end_date,
                    type_code,
                    type_code_system
                )
                VALUES (
                    %(id)s,
                    %(patient_id)s,
                    %(start_date)s,
                    %(end_date)s,
                    %(type_code)s,
                    %(type_code_system)s
                )
                """, encounter_data)

    count_performance['encounter'] = round(time.time() - start, 2)



def get_ndjson(link):
    response = requests.get(link)
    return response.json(cls=ndjson.Decoder)


if __name__ == '__main__':
    populate_tables()
