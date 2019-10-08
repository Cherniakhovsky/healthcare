import psycopg2

import common


def create_tables():
    """ Creates tables in already defined PostgreSQL"""
    try:
        conn = common.get_db_connection()
        cur = conn.cursor()
        execute_commands(cur)

        result = cur.fetchall()
        print(result)

        cur.close()
        conn.commit()


    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            cur.close()
            conn.close()


def execute_commands(cur):
    commands = (
        """
        CREATE TABLE IF NOT EXISTS patient (
            id serial primary key,
            source_id text NOT NULL,
            birth_date date,
            gender varchar(10),
            race_code varchar(20),
            race_code_system varchar(40),
            ethnicity_code varchar(20),
            ethnicity_code_system varchar(40),
            country varchar(30)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS encounter (
                id serial primary key,
                source_id text NOT NULL,
                patient_id int references patient(id) NOT NULL,
                start_date date NOT NULL,
                end_date date NOT NULL,
                type_code varchar(40),
                type_code_system varchar(40)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS procedure (
                id serial primary key,
                source_id text NOT NULL,
                patient_id int references patient(id) NOT NULL,
                encounter_id int references encounter(id),
                procedure_date timestamp with time zone NOT NULL,
                type_code varchar(40) NOT NULL,
                type_code_system varchar(40) NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS observation (
                id serial primary key,
                source_id text NOT NULL,
                patient_id int references patient(id) NOT NULL,
                encounter_id int references encounter(id),
                observation_date timestamp with time zone NOT NULL,
                type_code varchar(40) NOT NULL,
                type_code_system varchar(40) NOT NULL,
                value decimal NOT NULL,
                unit_code varchar(40),
                unit_code_system varchar(40)
        )
        """
    )
    for command in commands:
        cur.execute(command)

    # cur.execute("SELECT * FROM patient;")

    print('TABLES CREATED')

if __name__ == '__main__':
    create_tables()