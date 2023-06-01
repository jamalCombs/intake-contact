# README

## Prerequisites

Before running the code, ensure you have the following dependencies installed:
- pandas
- sqlalchemy
- psycopg2
- dotenv

## Installation

To install the required dependencies, you can use pip:

```
pip install pandas sqlalchemy psycopg2 json dotenv
```

## Database Configuration

Make sure to configure the database connection in the `connect_to_database()` function. Modify the connection string (`create_engine`) to match your database configuration.

## Usage

### Data Cleaning and Insertion

1. Open the Python script `intake_call.py` in an editor.
2. Modify the database connection configuration in the `connect_to_database()`.
3. Run the script using the Python interpreter.

The script will perform the following steps:
1. Connect to the database.
2. Drop the existing `intake_call` table (if exists).
3. Create the `intake_call` table with the defined schema.
4. Read and clean the data from the provided JSON file (`client_records.json`).
5. Insert the cleaned data into the database.
6. Close the database connection.

## Questions
1. Looking at the client_records.jsonl file, what questions do you have about the meaning of the data?
    1. How was the data sourced? Is it from a specific application, database, or external system? Were there any specific extraction methods or tools used?
    2. Are these names of "counselors" involved in the call?
    3. Are "transfer_timestamps" when the call was transferred to different counselors?
    4. Are "issues_discussed" predefined categories or free-text descriptions?
    5. Why does the "client_location" column contain only city names and not states?

2. What concerns do you see?
I noticed there are duplicate name entries such as 'Beth Chen' in the "counselors" column. Additionally, the purpose and meaning of the "transfer_timestamps" column, which contains a list of timestamps indicating transfers during the call, are not clearly defined. The "client_pronouns" column contains a single pronoun value ('They/Them') in list format. While this format is valid, it is advisable to establish a standardized approach for recording and managing pronouns to ensure consistency across different data entries. Also the "client_location" column only includes the value 'Columbus,' without specifying the state or country.

3. What choices have you made to clean the data?
By importing the data into a Pandas DataFrame, I leveraged the extensive functionality of pandas to effectively explore the structured tabular data. Pandas allowed me to efficiently perform tasks such as viewing, filtering, and transforming the data. 

I performed an initial data profiling by reviewing the data in subsets using Pandas, gradually increasing the number of rows viewed. This profiling step allowed me to gain insights into the overall characteristics of the dataset, including its structure, quality, and distribution. By understanding these aspects, I could identify potential issues, patterns, or anomalies in the data.

During the data cleaning process, I focused on four fields that required cleaning. The "counselors" and "transfer_timestamps" fields were currently represented as strings with lists in single quotes. To transform them into proper lists, I removed the quotes. Similarly, the "issues_discussed" field contained a list represented as a string with escaped double quotes. I converted it into a valid list format by replacing the escaped quotes.

I handled cases where empty lists and "datetime.datetime()" objects in the "transfer_timestamps" field were represented as strings like "[]". I checked for these instances and took appropriate actions, such as converting them None, as Postgres will handle them as Null values. To safely evaluate and convert the string representations of lists back into actual lists, I utilized the ast module literal_eval() function.

4. What choices have you made about the schema? Is this a relational database schema, or a big data one? (both choices are fine, just justify and explain yourself)
I chose to implement a relational database schema, specifically using Postgres. The data consists of structured and tabular information, making it a good fit for a relational model. By defining columns with specific data types and utilizing primary key constraints and foreign key references, I established and enforce relationships between entities and attributes. Postgres, being a widely-used relational database management system, offers robust features for data integrity, and query optimization. While big data solutions might be considered for massive-scale datasets or distributed processing requirements, those factors were not apparent in the given data. Therefore, a relational database schema using Postgres aligns well with the structured nature of the data and provides a solid foundation for managing relational data effectively.

5. If you were to scale your parsing code, what libraries/cloud technologies/strategies would you use to do so?
Using SQLAlchemy or Other Libraries: Instead of manually iterating over the DataFrame rows and inserting them one by one, you can consider using libraries like SQLAlchemy, which provide convenient ways to insert DataFrame data into a database table. SQLAlchemy supports various database backends, including PostgreSQL, and offers efficient bulk insert operations.

#### Future Recommendations
I recommend storing data in a structured format as it offers several advantages. It allows for easier manipulation and avoids complications related to string parsing and conversion. To ensure data organization and efficiency, I suggest normalizing the data by organizing it into separate tables based on entities and relationships. This approach promotes data consistency, reduces redundancy, and enables efficient querying and analysis. When dealing with missing values, it is essential to decide on a consistent approach, such as using NULL values or assigning default values to represent missing data. Lastly, adopting a descriptive and consistent naming convention for database tables, columns, and variables is highly recommended to enhance clarity and organization. To address the missing state values in the client_location field, I recommend performing data enrichment by leveraging the city names, such as using an automated process, you can look up and retrieve the corresponding state values from an external data source. 

## Retrieving Insight

#### Query: Create an output CSV with the data schema

Please see fetch_counselor_cases function for solution.

#### Query: The maximum number of concurrent cases handled by trevor at any time

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

#### Query: A list of counselors who dealt with more than one concurrent cases

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