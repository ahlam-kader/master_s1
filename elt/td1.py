# Producer

# from confluent_kafka import Producer
# import requests
# from bs4 import BeautifulSoup
# import pandas as pd

# def scrape_bcm_exchange_rates():
#     url = "https://www.bcm.mr/cours-de-change.html"

#     print("Fetching webpage...")
#     # Perform the request with SSL verification disabled
#     response = requests.get(url, verify=False)

#     if response.status_code != 200:
#         raise Exception(f"Failed to fetch the webpage. Status code: {response.status_code}")

#     print("Parsing HTML...")
#     soup = BeautifulSoup(response.content, 'html.parser')

#     # Find the specific table for "Cours de Reference"
#     table_div = soup.find("div", class_="rTable")
#     if not table_div:
#         raise Exception("Table not found in the page!")

#     # Extract headers
#     headers = [header.text.strip() for header in table_div.find("div", class_="rTableRow").find_all("div")]

#     # Extract data rows
#     rows = []
#     for row in table_div.find_all("div", class_="rTableRow")[1:]:  # Skip header row
#         cells = [cell.text.strip() for cell in row.find_all("div")]
#         rows.append(cells)

#     # Create a DataFrame for better visualization
#     df = pd.DataFrame(rows, columns=headers)
#     return df

# def send_topic(topic_name, data):
#     p = Producer({'bootstrap.servers': 'kafka:9092'})
#     p.produce(topic_name, value=data)
#     p.flush()

# if __name__ == "__main__":
#     try:
#         exchange_rates_df = scrape_bcm_exchange_rates()
#         print("Exchange Rates:")
#         print(exchange_rates_df)
#         send_topic('topic-step1', exchange_rates_df.to_json(orient='records'))
#         # Save to CSV (optional)
#         # exchange_rates_df.to_csv("exchange_rates.csv", index=False)
#         print("Exchange rates sent to Kafka")
#     except Exception as e:
#         print(f"Error: {e}")



# import requests
# from bs4 import BeautifulSoup
# import pandas as pd
# from confluent_kafka import Producer

# def scrape_bcm_exchange_rates():
#     url = "https://www.bcm.mr/cours-de-change.html"
    
#     print("Fetching webpage...")
#     # Perform the request with SSL verification disabled
#     response = requests.get(url, verify=False)

#     if response.status_code != 200:
#         raise Exception(f"Failed to fetch the webpage. Status code: {response.status_code}")

#     print("Parsing HTML...")
#     soup = BeautifulSoup(response.content, 'html.parser')

#     # Find the specific table for "Cours de Reference"
#     table_div = soup.find("div", class_="rTable")
#     if not table_div:
#         raise Exception("Table not found in the page!")

#     # Extract headers
#     headers = [header.text.strip() for header in table_div.find("div", class_="rTableRow").find_all("div")]

#     # Extract data rows
#     rows = []
#     for row in table_div.find_all("div", class_="rTableRow")[1:]:  # Skip header row
#         cells = [cell.text.strip() for cell in row.find_all("div")]
#         rows.append(cells)

#     # Create a DataFrame for better visualization
#     df = pd.DataFrame(rows, columns=headers)
#     return df

# def send_topic(topic_name, data):
#     p = Producer({'bootstrap.servers': 'kafka:9092'})
#     p.produce(topic_name, value=data)
#     p.flush()

# if __name__ == "__main__":
#     try:
#         exchange_rates_df = scrape_bcm_exchange_rates()
#         print("Exchange Rates:")
#         print(exchange_rates_df)
        
#         # Convert DataFrame to JSON and send it to Kafka
#         send_topic('exchange-rates-topic', exchange_rates_df.to_json(orient='records'))
#         print("Exchange rates sent to Kafka")
#     except Exception as e:
#         print(f"Error: {e}")




# import psycopg2

# # Remplacez "127.0.0.1" par "pgdb"
# conn = psycopg2.connect(
#     host="pgdb",  # Hôte est maintenant le nom du service Docker pour PostgreSQL
#     database="etldb",
#     user="admin",
#     password="adminx01"
# )

# # Ensuite, votre code de création de table
# cursor = conn.cursor()

# try:
#     cursor.execute("""
#         CREATE TABLE exchange (
#             date DATE,
#             currency_code VARCHAR,
#             currency_unit INT,
#             currency_rate FLOAT
#         );
#     """)
#     conn.commit()
#     print("Table 'exchange' créée avec succès !")
# except Exception as e:
#     print(f"Erreur lors de la création de la table : {e}")
# finally:
#     cursor.close()
#     conn.close()



from confluent_kafka import Consumer, KafkaException
import psycopg2
import json
import pandas as pd

# Connect to PostgreSQL
def connect_to_postgres():
    return psycopg2.connect(
        host="pgdb",  # Name of your PostgreSQL service in Docker (pgdb)
        database="etldb",
        user="admin",
        password="adminx01"
    )

# Insert data into PostgreSQL
def insert_into_postgres(data):
    conn = connect_to_postgres()
    cursor = conn.cursor()
    try:
        for record in data:
            # Assuming record is a dictionary
            cursor.execute("""
                INSERT INTO exchange (date, currency_code, currency_unit, currency_rate)
                VALUES (%s, %s, %s, %s)
            """, (record['date'], record['currency_code'], record['currency_unit'], record['currency_rate']))
        conn.commit()
    except Exception as e:
        print(f"Error inserting into PostgreSQL: {e}")
    finally:
        cursor.close()
        conn.close()

# Kafka consumer function
def consume_from_kafka(topic_name):
    consumer = Consumer({
        'bootstrap.servers': 'localhost:9092',  # Kafka broker address
        'group.id': 'exchange_rates_group',
        'auto.offset.reset': 'earliest'
    })

    consumer.subscribe([topic_name])

    try:
        while True:
            msg = consumer.poll(1.0)  # Wait for a message
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            else:
                # Parse the JSON message and insert into PostgreSQL
                data = json.loads(msg.value().decode('utf-8'))
                data_df = pd.read_json(data)
                data_records = data_df.to_dict(orient='records')  # Convert to list of dictionaries
                insert_into_postgres(data_records)
                print("Data inserted into PostgreSQL!")
    except KeyboardInterrupt:
        print("Consumer stopped.")
    finally:
        consumer.close()

# Start consuming from Kafka
consume_from_kafka('exchange_rates_topic')
