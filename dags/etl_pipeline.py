import requests
import psycopg2
import time

def fetch_api_data():
    """Fetch random user data from the API."""
    res = requests.get("https://randomuser.me/api/")
    return res.json()['results'][0]

def transform_data(data):
    """Transform data by extracting gender, full name, and email."""
    return {
        "gender": data.get("gender"),
        "name": f"{data['name']['title']} {data['name']['first']} {data['name']['last']}",
        "email": data.get("email"),
    }

def connect_with_retry(retries=5, delay=5):
    """Retry logic for connecting to PostgreSQL."""
    for attempt in range(retries):
        try:
            connection = psycopg2.connect(
                host="postgres",
                database="datawarehouse",
                user="airflow",
                password="airflow"
            )
            return connection
        except psycopg2.OperationalError as e:
            if attempt < retries - 1:
                print(f"Connection failed, retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                raise e

def load_data_to_postgres(data):
    """Load transformed data into PostgreSQL."""
    connection = connect_with_retry()
    cursor = connection.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS random_users (
            id SERIAL PRIMARY KEY,
            gender TEXT,
            name TEXT,
            email TEXT
        )
    """)
    connection.commit()
    cursor.execute(
        "INSERT INTO random_users (gender, name, email) VALUES (%s, %s, %s)",
        (data["gender"], data["name"], data["email"])
    )
    connection.commit()
    cursor.close()
    connection.close()

def run_etl():
    """Run the ETL process."""
    raw_data = fetch_api_data()
    transformed_data = transform_data(raw_data)
    load_data_to_postgres(transformed_data)
