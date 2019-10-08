import ndjson
import psycopg2
import psycopg2.extras
import requests
import time
import json

import common


def populate_tables():
    """
    Inserts medical example data to four tables retrieved from link
    """
    report = {
        'insert_time': {
            'patient': 0,
            'encounter': 0,
            'procedure': 0,
            'observation': 0
            }
    }
    try:
        conn = common.get_db_connection()
        cur = conn.cursor()

        _populate_patient_table(cur, report)

        _populate_encounter_table(cur, report)

        _populate_procedure_table(cur, report)

        _populate_observation_table(cur, report)

        print('\nTime spent for populating tables: ')
        print(f'Patient - {report["insert_time"]["patient"]} sec')
        print(f'Encounter - {report["insert_time"]["encounter"]} sec')
        print(f'Procedure - {report["insert_time"]["procedure"]} sec')
        print(f'Observation - {report["insert_time"]["observation"]} sec')

        cur.close()
        conn.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            cur.close()
            conn.close()


def _populate_patient_table(cur, report):
    """
    Inserts patient data
    """

    patients_link = 'https://raw.githubusercontent.com/smart-on-fhir/flat-fhir-files/master/r3/Patient.ndjson'
    patients_data = get_ndjson(patients_link)

    start = time.time()
    for data in patients_data:

        new_data = dict()

        obligatory_result = _handle_obligatory_patient_fields(cur, data, new_data)

        if not obligatory_result:
            continue

        _handle_optional_patient_fields(cur, data, new_data)

        _save_patients(cur, new_data)

    print('Patient TABLE populated')

    report['insert_time']['patient'] = round(time.time() - start, 2)


def _populate_encounter_table(cur, report):
    """
    Inserts encounter data
    """

    encounter_link = 'https://raw.githubusercontent.com/smart-on-fhir/flat-fhir-files/master/r3/Encounter.ndjson'
    encounters_data = get_ndjson(encounter_link)

    start = time.time()
    for data in encounters_data:

        new_data = dict()

        obligatory_result = _handle_obligatory_encounter_fields(cur, data, new_data)

        if not obligatory_result:
            continue

        _handle_optional_encounter_fields(cur, data, new_data)

        _save_encounters(cur, new_data)

    print('Encounter TABLE populated')

    report['insert_time']['encounter'] = round(time.time() - start, 2)


def _populate_procedure_table(cur, report):
    """
    Inserts procedure data
    """

    procedure_link = 'https://raw.githubusercontent.com/smart-on-fhir/flat-fhir-files/master/r3/Procedure.ndjson'
    procedures_data = get_ndjson(procedure_link)

    start = time.time()
    for data in procedures_data:
        new_data = dict()

        obligatory_result = _handle_obligatory_procedure_fields(cur, data, new_data)

        if not obligatory_result:
            continue

        _handle_optional_procedure_fields(cur, data, new_data)

        _save_procedures(cur, new_data)

    print('Procedure TABLE populated')

    report['insert_time']['procedure'] = round(time.time() - start, 2)


def _populate_observation_table(cur, report):
    """
    Inserts observation data
    """

    procedure_link = 'https://raw.githubusercontent.com/smart-on-fhir/flat-fhir-files/master/r3/Observation.ndjson'
    procedures_data = get_ndjson(procedure_link)

    start = time.time()
    for data in procedures_data:
        new_data = dict()

        if data.get('component'):

            for component in data['component']:
                new_data = dict()

                obligatory_result = _handle_obligatory_observation_fields_with_component(
                    cur, data, new_data, component)

                if not obligatory_result:
                    continue

                _handle_optional_observation_fields_with_component(
                    cur, data, new_data, component)

                _save_observations(cur, new_data)

        else:

            obligatory_result = _handle_obligatory_observation_fields(
                cur, data, new_data)

            if not obligatory_result:
                continue

            _handle_optional_observation_fields(cur, data, new_data)

            _save_observations(cur, new_data)

    print('Observation TABLE populated')

    report['insert_time']['observation'] = round(time.time() - start, 2)


def _handle_obligatory_patient_fields(cur, data, new_data):

    try:
        # source_id
        new_data['id'] = data['id']
        return True

    except:
        with open("logs/skipped_patients.ndjson", "a+") as f:
            dict_to_ndjson(data, f)
        return False


def _handle_optional_patient_fields(cur, data, new_data):
        try:
            new_data['birth_date'] = data['birthDate']
        except:
            new_data['birth_date'] = None

        try:
            new_data['gender'] = data['gender']
        except:
            new_data['gender'] = None

        try:
            new_data['race_code'] = data['extension'][0]['valueCodeableConcept']['coding'][0]['code']
        except:
            new_data['race_code'] = None

        try:
            new_data['race_code_system'] = data['extension'][0]['valueCodeableConcept']['coding'][0]['system']
        except:
            new_data['race_code_system'] = None

        try:
            new_data['ethnicity_code'] = data['extension'][1]['valueCodeableConcept']['coding'][0]['code']
        except:
            new_data['ethnicity_code'] = None

        try:
            new_data['ethnicity_code_system'] = data['extension'][1]['valueCodeableConcept']['coding'][0]['system']
        except:
            new_data['ethnicity_code_system'] = None

        try:
            new_data['country'] = data['address'][0]['country']
        except:
            new_data['country'] = None


def _save_patients(cur, new_data):
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
            %(birth_date)s,
            %(country)s,
            %(race_code)s,
            %(race_code_system)s,
            %(ethnicity_code)s,
            %(ethnicity_code_system)s
        )
        """, new_data)


def _handle_obligatory_encounter_fields(cur, data, new_data):

    try:
        # source_id
        new_data['id'] = data['id']

        # patient_id
        patient_source_id = data['subject']['reference'].split('/')[-1]
        query = f"SELECT id FROM patient WHERE patient.source_id = '{patient_source_id}'"
        cur.execute(query)
        new_data['patient_id'] = cur.fetchone()[0]

        # start_date
        new_data['start_date'] = data['period']['start']

        # end_date
        new_data['end_date'] = data['period']['end']

        return True

    except:
        with open("logs/skipped_encounters.ndjson", "a+") as f:
            dict_to_ndjson(data, f)
        return False


def _handle_optional_encounter_fields(cur, data, new_data):
        try:
            new_data['type_code'] = data['type'][0]['coding'][0]['code']
        except:
            new_data['type_code'] = None

        try:
            new_data['type_code_system'] = data['type'][0]['coding'][0]['system']
        except:
            new_data['type_code_system'] = None


def _save_encounters(cur, new_data):
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
        """, new_data)


def _handle_obligatory_procedure_fields(cur, data, new_data):

    try:
        # source_id
        new_data['source_id'] = data['id']

        # patient_id
        patient_source_id = data['subject']['reference'].split('/')[-1]
        query = f"SELECT id FROM patient WHERE patient.source_id = '{patient_source_id}'"
        cur.execute(query)
        new_data['patient_id'] = cur.fetchone()[0]

        # procedure_date
        if data.get('performedDateTime'):
            new_data['procedure_date'] = data['performedDateTime']
        else:
            new_data['procedure_date'] = data['performedPeriod']['start']

        # type_code
        new_data['type_code'] = data['code']['coding'][0]['code']

        # type_code_system
        new_data['type_code_system'] = data['code']['coding'][0]['system']

        return True

    except:
        with open("logs/skipped_procedures.ndjson", "a+") as f:
            dict_to_ndjson(data, f)
        return False


def _handle_optional_procedure_fields(cur, data, new_data):

    # encounter_id
    try:
        # print(new_data['subject']['reference'].split('/')[-1])
        encounter_source_id = data['context']['reference'].split('/')[-1]
        query = f"SELECT id FROM encounter WHERE encounter.source_id = '{encounter_source_id}'"
        cur.execute(query)
        new_data['encounter_id'] = cur.fetchone()[0]
    except:
        new_data['encounter_id'] = None


def _save_procedures(cur, new_data):
    cur.execute("""
                INSERT INTO procedure (
                    source_id,
                    patient_id,
                    encounter_id,
                    procedure_date,
                    type_code,
                    type_code_system
                )
                VALUES (
                    %(source_id)s,
                    %(patient_id)s,
                    %(encounter_id)s,
                    %(procedure_date)s,
                    %(type_code)s,
                    %(type_code_system)s
                )
                """, new_data)


def _handle_obligatory_observation_fields(cur, data, new_data):
    try:
        # source_id
        new_data['source_id'] = data['id']

        # patient_id
        patient_source_id = data['subject']['reference'].split('/')[-1]
        query = f"SELECT id FROM patient WHERE patient.source_id = '{patient_source_id}'"
        cur.execute(query)
        new_data['patient_id'] = cur.fetchone()[0]

        # observation_date
        new_data['observation_date'] = data['effectiveDateTime']

        # type_code
        new_data['type_code'] = data['code']['coding'][0]['code']

        # type_code_system
        new_data['type_code_system'] = data['code']['coding'][0]['system']

        # value
        new_data['value'] = data['valueQuantity']['value']

        return True

    except:
        with open("logs/skipped_observations.ndjson", "a+") as f:
            dict_to_ndjson(data, f)
        return False


def _handle_obligatory_observation_fields_with_component(cur, data, new_data, component):

    try:
        # source_id
        new_data['source_id'] = data['id']

        # patient_id
        patient_source_id = data['subject']['reference'].split('/')[-1]
        query = f"SELECT id FROM patient WHERE patient.source_id = '{patient_source_id}'"
        cur.execute(query)
        new_data['patient_id'] = cur.fetchone()[0]

        # observation_date
        new_data['observation_date'] = data['effectiveDateTime']

        # type_code component[].code.coding[0].code
        new_data['type_code'] = component['code']['coding'][0]['code']

        # type_code_system component[].code.coding[0].system
        new_data['type_code_system'] = component['code']['coding'][0]['system']

        # value component[].valueQuantity.value
        new_data['value'] = component['valueQuantity']['value']

        return True

    except:
        with open("logs/skipped_observations.ndjson", "a+") as f:
            dict_to_ndjson(data, f)
        return False



def _handle_optional_observation_fields(cur, data, new_data):

    # encounter_id
    try:
        # print(new_data['subject']['reference'].split('/')[-1])
        encounter_source_id = data['context']['reference'].split('/')[-1]
        query = f"SELECT id FROM encounter WHERE encounter.source_id = '{encounter_source_id}'"
        cur.execute(query)
        new_data['encounter_id'] = cur.fetchone()[0]
    except:
        new_data['encounter_id'] = None

    # unit_code
    try:
        new_data['unit_code'] = data['valueQuantity']['unit']
    except:
        new_data['unit_code'] = None

    # unit_code_system
    try:
        new_data['unit_code_system'] = data['valueQuantity']['system']
    except:
        new_data['unit_code_system'] = None


def _handle_optional_observation_fields_with_component(cur, data, new_data, component):

    # encounter_id
    try:
        # print(new_data['subject']['reference'].split('/')[-1])
        encounter_source_id = data['context']['reference'].split('/')[-1]
        query = f"SELECT id FROM encounter WHERE encounter.source_id = '{encounter_source_id}'"
        cur.execute(query)
        new_data['encounter_id'] = cur.fetchone()[0]
    except:
        new_data['encounter_id'] = None

    # unit_code component[].valueQuantity.unit
    try:
        new_data['unit_code'] = component['valueQuantity']['unit']
    except:
        new_data['unit_code'] = None

    # unit_code_system component[].valueQuantity.system
    try:
        new_data['unit_code_system'] = component['valueQuantity']['system']
    except:
        new_data['unit_code_system'] = None


def _save_observations(cur, new_data):
    cur.execute("""
            INSERT INTO observation (
                source_id,
                patient_id,
                encounter_id,
                observation_date,
                type_code,
                type_code_system,
                value,
                unit_code,
                unit_code_system
            )
            VALUES (
                %(source_id)s,
                %(patient_id)s,
                %(encounter_id)s,
                %(observation_date)s,
                %(type_code)s,
                %(type_code_system)s,
                %(value)s,
                %(unit_code)s,
                %(unit_code_system)s
            )
            """, new_data)


def get_ndjson(link):
    response = requests.get(link)
    return response.json(cls=ndjson.Decoder)

def dict_to_ndjson(dict, file):
    json.dump(dict, file)
    file.write('\n')


if __name__ == '__main__':
    populate_tables()
