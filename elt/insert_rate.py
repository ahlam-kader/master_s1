import psycopg2
import requests
from datetime import datetime

def fetch_and_insert_rate():
    response = requests.get("https://api.exchangerate-api.com/v4/latest/USD")
    response.raise_for_status()
    rates = response.json().get('rates', {})
    usd_to_mru_rate = rates.get('MRU')

    if usd_to_mru_rate is None:
        raise ValueError("MRU rate not found in the API response")

    connection = psycopg2.connect(
        dbname="etldb",
        user="admin",
        password="adminx01",
        host="pgdb",
        port=5432
    )
    cursor = connection.cursor()
    timestamp = datetime.now()

    query = """
    INSERT INTO exchange_rates (currency, rate, updated_at)
    VALUES (%s, %s, %s);
    """
    cursor.execute(query, ('MRU', usd_to_mru_rate, timestamp))

    connection.commit()
    cursor.close()
    connection.close()

if __name__ == "__main__":
    fetch_and_insert_rate()
