import ndjson
import psycopg2
import psycopg2.extras
import requests
import time
import json

from app import common


def populate_tables():
    """
    Inserts medical example data to four tables retrieved from link
    """
    report = {
        'insert_time': {
            'patients': 0,
            'encounters': 0,
            'procedures': 0,
            'observations': 0
            },
        'skipped': {
            'patients': 0,
            'encounters': 0,
            'procedures': 0,
            'observations': 0,
            'observations_with_component': 0
            }
    }
    try:
        conn = common.get_db_connection()
        cur = conn.cursor()

        _populate_patient_table(cur, report)

        _populate_encounter_table(cur, report)

        _populate_procedure_table(cur, report)

        _populate_observation_table(cur, report)

        # print('\nTime spent for populating tables: ')
        # print(f'Patient - {report["insert_time"]["patients"]} sec')
        # print(f'Encounter - {report["insert_time"]["encounters"]} sec')
        # print(f'Procedure - {report["insert_time"]["procedures"]} sec')
        # print(f'Observation - {report["insert_time"]["observations"]} sec')

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
    obligatory_fields = {'source_id'}
    log_file = 'skipped_patients.ndjson'

    start = time.time()
    for data in patients_data:

        new_data = dict()

        _handle_patient_fields(data, new_data)

        all_obligatory_fields = _check_all_obligatory_fields_present(
            obligatory_fields, new_data)

        if not all_obligatory_fields:
            _skip_saving_and_write_logs(log_file)
            report['skipped']['patients'] += 1
            continue

        _save_patients(cur, new_data)

    print('Patient TABLE populated')
    print(f'- There were {report["skipped"]["patients"]} skipped patients!')

    report['insert_time']['patients'] = round(time.time() - start, 2)
    print(f'- It took {report["insert_time"]["patients"]} sec')


def _handle_patient_fields(data, new_data):

    try:
        new_data['source_id'] = data['id']
    except:
        pass

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
            %(source_id)s,
            %(gender)s,
            %(birth_date)s,
            %(country)s,
            %(race_code)s,
            %(race_code_system)s,
            %(ethnicity_code)s,
            %(ethnicity_code_system)s
        )
        """, new_data)


def _populate_encounter_table(cur, report):
    """
    Inserts encounter data
    """

    encounter_link = 'https://raw.githubusercontent.com/smart-on-fhir/flat-fhir-files/master/r3/Encounter.ndjson'
    encounters_data = get_ndjson(encounter_link)
    obligatory_fields = {'source_id', 'patient_id', 'start_date', 'end_date'}
    log_file = 'skipped_encounters.ndjson'

    start = time.time()
    for data in encounters_data:

        new_data = dict()

        _handle_encounter_fields(data, new_data)

        _handle_encounter_referenced_fields(cur, data, new_data)

        all_obligatory_fields = _check_all_obligatory_fields_present(
            obligatory_fields, new_data)

        if not all_obligatory_fields:
            _skip_saving_and_write_logs(log_file, data)
            report['skipped']['encounters'] += 1
            continue

        _save_encounters(cur, new_data)

    print('Encounter TABLE populated')
    print(f'- There were {report["skipped"]["encounters"]} skipped encounters!')

    report['insert_time']['encounters'] = round(time.time() - start, 2)
    print(f'- It took {report["insert_time"]["encounters"]} sec')


def _handle_encounter_fields(data, new_data):

    try:
        new_data['source_id'] = data['id']

        new_data['start_date'] = data['period']['start']

        new_data['end_date'] = data['period']['end']
    except:
        pass

    try:
        new_data['type_code'] = data['type'][0]['coding'][0]['code']
    except:
        new_data['type_code'] = None

    try:
        new_data['type_code_system'] = data['type'][0]['coding'][0]['system']
    except:
        new_data['type_code_system'] = None


def _handle_encounter_referenced_fields(cur, data, new_data):
    _get_patient_id(cur, data, new_data)


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
            %(source_id)s,
            %(patient_id)s,
            %(start_date)s,
            %(end_date)s,
            %(type_code)s,
            %(type_code_system)s
        )
        """, new_data)



def _populate_procedure_table(cur, report):
    """
    Inserts procedure data
    """

    procedure_link = 'https://raw.githubusercontent.com/smart-on-fhir/flat-fhir-files/master/r3/Procedure.ndjson'
    procedures_data = get_ndjson(procedure_link)
    obligatory_fields = {'source_id', 'patient_id', 'procedure_date', 'type_code', 'type_code_system'}
    log_file = 'skipped_procedures.ndjson'

    start = time.time()
    for data in procedures_data:
        new_data = dict()

        _handle_procedure_fields(data, new_data)

        _handle_procedure_referenced_fields(cur, data, new_data)

        all_obligatory_fields = _check_all_obligatory_fields_present(
            obligatory_fields, new_data)

        if not all_obligatory_fields:
            _skip_saving_and_write_logs(log_file, data)
            report['skipped']['procedures'] += 1
            continue

        _save_procedures(cur, new_data)


    print('Procedure TABLE populated')
    print(f'- There were {report["skipped"]["procedures"]} skipped procedures!')

    report['insert_time']['procedures'] = round(time.time() - start, 2)
    print(f'- It took {report["insert_time"]["procedures"]} sec')


def _handle_procedure_fields(data, new_data):

    try:
        new_data['source_id'] = data['id']

        if data.get('performedDateTime'):
            new_data['procedure_date'] = data['performedDateTime']
        else:
            new_data['procedure_date'] = data['performedPeriod']['start']

        new_data['type_code'] = data['code']['coding'][0]['code']

        new_data['type_code_system'] = data['code']['coding'][0]['system']

        return True

    except:
        with open("logs/skipped_procedures.ndjson", "a+") as f:
            dict_to_ndjson(data, f)
        return False


def _handle_procedure_referenced_fields(cur, data, new_data):
    _get_patient_id(cur, data, new_data)
    _get_encounter_id(cur, data, new_data)


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


def _populate_observation_table(cur, report):
    """
    Inserts observation data
    """

    procedure_link = 'https://raw.githubusercontent.com/smart-on-fhir/flat-fhir-files/master/r3/Observation.ndjson'
    procedures_data = get_ndjson(procedure_link)
    obligatory_fields = {'source_id', 'patient_id', 'observation_date', 'type_code', 'type_code_system', 'value'}
    log_file = 'skipped_observations.ndjson'

    start = time.time()
    for data in procedures_data:
        new_data = dict()

        if data.get('component'):

            for component in data['component']:
                new_data = dict()

                _handle_observation_fields_with_component(
                    data, new_data, component)

                _handle_observation_referenced_fields_with_component(
                    cur, data, new_data)

                all_obligatory_fields = _check_all_obligatory_fields_present(
                    obligatory_fields, new_data)

                if not all_obligatory_fields:
                    _skip_saving_and_write_logs(log_file, data)
                    report['skipped']['observations_with_component'] += 1
                    continue

                _save_observations(cur, new_data)

        else:

            _handle_observation_fields(data, new_data)

            _handle_observation_referenced_fields(cur, data, new_data)

            all_obligatory_fields = _check_all_obligatory_fields_present(
                obligatory_fields, new_data)

            if not all_obligatory_fields:
                _skip_saving_and_write_logs(log_file, data)
                report['skipped']['observations'] += 1
                continue

            _save_observations(cur, new_data)

    print('Observation TABLE populated')
    print(f'- There were {report["skipped"]["observations"]} skipped observations '
          f'and {report["skipped"]["observations_with_component"]} skipped '
          f'observations with component')

    report['insert_time']['observations'] = round(time.time() - start, 2)
    print(f'- It took {report["insert_time"]["observations"]} sec')


def _handle_observation_fields(data, new_data):
    try:
        new_data['source_id'] = data['id']

        new_data['observation_date'] = data['effectiveDateTime']

        new_data['type_code'] = data['code']['coding'][0]['code']

        new_data['type_code_system'] = data['code']['coding'][0]['system']

        new_data['value'] = data['valueQuantity']['value']
    except:
        pass

    try:
        new_data['unit_code'] = data['valueQuantity']['unit']
    except:
        new_data['unit_code'] = None

    try:
        new_data['unit_code_system'] = data['valueQuantity']['system']
    except:
        new_data['unit_code_system'] = None


def _handle_observation_referenced_fields(cur, data, new_data):
    _get_patient_id(cur, data, new_data)
    _get_encounter_id(cur, data, new_data)


def _handle_observation_referenced_fields_with_component(cur, data, new_data):
    _get_patient_id(cur, data, new_data)
    _get_encounter_id(cur, data, new_data)


def _handle_observation_fields_with_component(data, new_data, component):

    try:
        new_data['source_id'] = data['id']

        new_data['observation_date'] = data['effectiveDateTime']

        new_data['type_code'] = component['code']['coding'][0]['code']

        new_data['type_code_system'] = component['code']['coding'][0]['system']

        new_data['value'] = component['valueQuantity']['value']
    except:
        pass

    try:
        new_data['unit_code'] = component['valueQuantity']['unit']
    except:
        new_data['unit_code'] = None

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


def _check_all_obligatory_fields_present(obligatory_fields, new_data):
    """
    Check that all required fields are present. If not ndjson data won't
    be saved in DB but this data is goint to be save in a log file for
    having a possibility to review skipped data afterwards.
    """
    if obligatory_fields - set(new_data.keys()):
        return False
    return True


def _skip_saving_and_write_logs(log_file, data):
    """
    As not all obligatory fields are present data is not going to
    be saved in db but is saved in this log file.
    """
    with open(f'logs/{log_file}', 'a+') as f:
        dict_to_ndjson(data, f)


def _get_patient_id(cur, data, new_data):
    try:
        patient_source_id = data['subject']['reference'].split('/')[-1]
        query = f"SELECT id FROM patient WHERE patient.source_id = '{patient_source_id}'"
        cur.execute(query)
        new_data['patient_id'] = cur.fetchone()[0]
    except:
        pass


def _get_encounter_id(cur, data, new_data):
    try:
        encounter_source_id = data['context']['reference'].split('/')[-1]
        query = f"SELECT id FROM encounter WHERE encounter.source_id = '{encounter_source_id}'"
        cur.execute(query)
        new_data['encounter_id'] = cur.fetchone()[0]
    except:
        new_data['encounter_id'] = None


def get_ndjson(link):
    response = requests.get(link)
    return response.json(cls=ndjson.Decoder)


def dict_to_ndjson(dict, file):
    json.dump(dict, file)
    file.write('\n')


if __name__ == '__main__':
    populate_tables()
