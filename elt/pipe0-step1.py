import requests
import psycopg2
from datetime import datetime

def fetch_btc_price():
    url = "https://api.coindesk.com/v1/bpi/currentprice/USD.json"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        return data['bpi']['USD']['rate_float']
    else:
        raise Exception("Failed to fetch BTC price")

def save_to_db(price):
    # Database connection parameters
    db_params = {
        'dbname': 'etldb',
        'user': 'admin',
        'password': 'adminx01',
        'host': 'pgdb',
        'port': 5432
    }

    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()

        # Ensure the table exists
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS btc_price (
            date DATE NOT NULL,
            currency VARCHAR(10) NOT NULL,
            price NUMERIC NOT NULL,
            insert_time TIMESTAMP NOT NULL
        );
        ''')

        # Insert data
        today_date = datetime.now().date()
        insert_time = datetime.now()
        currency = 'USD'

        cursor.execute(
            '''
            INSERT INTO btc_price (date, currency, price, insert_time)
            VALUES (%s, %s, %s, %s);
            ''',
            (today_date, currency, price, insert_time)
        )

        # Commit the transaction
        conn.commit()
        print("Data inserted successfully")

    except Exception as e:
        print("Error:", e)

    finally:
        # Close the connection
        if conn:
            cursor.close()
            conn.close()

if __name__ == "__main__":
    try:
        # Fetch the BTC price
        btc_price = fetch_btc_price()
        print(f"Fetched BTC price: {btc_price} USD")

        # Save the BTC price to the database
        save_to_db(btc_price)

    except Exception as e:
        print("Error:", e)
