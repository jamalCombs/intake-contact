# README

This straightforward Python script serves as a tool for handling an intake contact database. Its primary purpose revolves around tidying up JSON data and seamlessly integrating it into a PostgreSQL database. Additionally, it demonstrates the process of generating insightful reports. 

## Prerequisites

Before running the script, make sure to:

1. Install the required Python packages by running `pip install pandas sqlalchemy psycopg2-binary dotenv`.

2. Create a PostgreSQL database and provide the connection URL in a `.env` file in the same directory as this script. Example `.env` content:

   ```
   DATABASE_URL=postgresql://username:password@localhost/database_name
   ```

3. Prepare a JSON file (`client_records.json`) containing the data you want to insert into the database.

## Script Structure

- Import necessary libraries/modules.
- Define the `IntakeContact` SQLAlchemy model to represent the database table structure.
- Create functions for database connection and management (`connect_to_database`, `create_table`, `drop_table`).
- Implement functions to process and insert data into the database (`insert_data`).
- Define functions for data retrieval (`fetch_data`, `fetch_counselor_cases`).
- Create a `main` function to execute the data insertion process.

## How to Use

1. Ensure the prerequisites are met.

2. Place the `client_records.json` file in the same directory as this script.

3. Run the script using `python script_name.py` (replace `script_name.py` with the actual script filename).

4. The script will read data from the JSON file, process it, and insert it into the PostgreSQL database.

5. After successful insertion, you can use the `fetch_data` and `fetch_counselor_cases` functions to retrieve specific data from the database.

6. Report data will be saved in a CSV file named `counselor_cases.csv`.

7. If any errors occur during execution, error messages will be logged.

## Note

- Ensure the PostgreSQL database is correctly set up with the necessary table structure to match the `IntakeContact` model.

- This script is a basic template and can be extended or modified to suit your specific database and data processing needs.

- Be cautious when handling sensitive data and consider security best practices when using databases.

### Database Configuration

Make sure to configure the database connection in the `connect_to_database()` function. Modify the connection string (`create_engine`) to match your database configuration.

## Usage Data Cleaning and Insertion

### Retrieving Insight

#### Example of data:
``` json
{
  "time_call_began": "2023-01-01 01:06:00",
  "time_call_ended": "2023-01-01 02:05:00",
  "counselors": "['Hans Yamada']",
  "transfer_timestamps": "[]",
  "issues_discussed": "['Gay/Lesbian Identity']",
  "call_rating": 2,
  "initial_risk_level": 3,
  "client_pronouns": "['He/Him']",
  "client_name": "Marsha Mohammed Chen",
  "client_location": "Boston"
}
```

#### Query: The maximum number of concurrent cases handled by Trevor at any time

```sql
SELECT MAX(concurrent_cases) AS max_concurrent_cases
FROM (
  SELECT COUNT(*) AS concurrent_cases
  FROM (
    SELECT unnest(counselors) AS counselor, unnest(transfer_timestamps) AS transfer_timestamp
    FROM intake_call
  ) AS counselor_cases
  GROUP BY transfer_timestamp
) AS concurrent_cases;
```

#### Query: A list of counselors who dealt with more than one concurrent case

```sql
WITH counselor_cases AS (
  SELECT counselor,
         transfer_timestamp,
         row_number() OVER (PARTITION BY transfer_timestamp ORDER BY id) AS case_number
  FROM intake_call,
       unnest(counselors) AS counselor,
       unnest(transfer_timestamps) AS transfer_timestamp
)
SELECT counselor
FROM counselor_cases
GROUP BY counselor
HAVING count(*) > 1;
```

#### Query: The average risk level of people who use She/They pronouns

```sql
SELECT AVG(initial_risk_level) AS average_risk_level
FROM intake_call
WHERE 'She/Her' = ANY (client_pronouns) OR 'They/Them' = ANY (client_pronouns);
```