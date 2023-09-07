import os
import re
import ast
import time
import logging
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, DateTime, text, ARRAY
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import declarative_base
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from the file
load_dotenv('.env')

Base = declarative_base()

class IntakeContact(Base):
    __tablename__ = 'intake_contact'

    id = Column(Integer, primary_key=True)
    time_call_began = Column(DateTime)
    time_call_ended = Column(DateTime)
    counselors = Column(ARRAY(String))
    transfer_timestamps = Column(ARRAY(DateTime))
    issues_discussed = Column(ARRAY(String))
    call_rating = Column(Integer)
    initial_risk_level = Column(Integer)
    client_pronouns = Column(ARRAY(String))
    client_name = Column(String)
    client_location = Column(String)

def connect_to_database():
    try:
        database_url = os.getenv('DATABASE_URL')
        engine = create_engine(database_url)
        Session = sessionmaker(bind=engine)
        session = Session()
        return session
    except Exception as e:
        logging.error(f"Error connecting to the database: {str(e)}")
        raise

def create_table(session):
    Base.metadata.create_all(session.get_bind())

def drop_table(session):
    Base.metadata.drop_all(session.get_bind())

def convert_datetime(arr):
    # Extract and convert date and time values from a given array using regular expressions and datetime
    pattern = r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}'
    matches = re.findall(pattern, arr)
    if matches:
        return [
            datetime.strptime(match, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')
            for match in matches
        ]
    else:
        return None

def process_records(records, keys_to_eval):
    # Evaluate specific keys within a list of records and safely update the records to ensure safe evaluation of string representations of Python literals.
    for record in records:
        record.update({
            key: ast.literal_eval(record.get(key)) if isinstance(record.get(key), str) and record.get(key) != '{}' else record.get(key)
            for key in keys_to_eval
        })
    
def insert_data(session, json_file):
    try:
        df = pd.read_json(json_file, lines=True)
        # Convert date and time values
        df['transfer_timestamps'] = df['transfer_timestamps'].apply(convert_datetime)

        # Convert the DataFrame to a list of dictionaries for Postgres insertion
        records = df.to_dict(orient='records')

        # List of keys in the records that need to be evaluated.
        keys_to_eval = ['counselors', 'transfer_timestamps', 'issues_discussed', 'client_pronouns']

        process_records(records, keys_to_eval)

        # Measure the duration of the insertion process
        start_time = time.time()

        # Bulk insert the records
        session.bulk_insert_mappings(IntakeContact, records, return_defaults=True)

        session.commit()
        session.close()

        elapsed_time = time.time() - start_time

        # Print the elapsed time
        print(f'Data insertion took {elapsed_time:.2f} seconds.')

    except FileNotFoundError as error:
        logging.error("Error while reading JSON file: File not found.")
    except ValueError as error:
        logging.error("Error while inserting data into the database: Invalid value.")
    except Exception as error:
        logging.error("Error occurred: " + str(error))

def fetch_data(session):
    # Fetch data from the table
    rows = session.query(IntakeContact).filter_by(id=63).all()

    return rows

def fetch_counselor_cases(session):
    try:
        # Execute the query to fetch the data and calculate the required fields
        query = text(
            """
                SELECT
                    unnest(counselors) AS "COUNSELOR NAME",
                    date_trunc('day', time_call_began) AS "DAY",
                    COUNT(*) AS "NUMBER OF CASES",
                    ROUND(AVG(initial_risk_level), 2) AS "AVERAGE RISK LEVEL",
                    ROUND(AVG(call_rating), 2) AS "AVERAGE RATING"
                FROM
                    intake_contact
                GROUP BY
                    unnest(counselors),
                    date_trunc('day', time_call_began);
            """
        )
        result = session.execute(query)
        rows = result.fetchall()
        output_file = 'counselor_cases.csv'

        # Define the field names for the CSV file
        field_names = ['COUNSELOR NAME', 'DAY', 'NUMBER OF CASES', 'AVERAGE RISK LEVEL', 'AVERAGE RATING']

        df = pd.DataFrame(rows, columns=field_names)
        df.to_csv(output_file, index=False)
    except Exception as e:
        logging.error(f"Error fetching counselor cases: {str(e)}")
        raise

def main():
    session = connect_to_database()
    try:
        insert_data(session, 'client_records.json')
    except Exception as e:
        logging.error(f"Main function error: {str(e)}")

if __name__ == '__main__':
    main()