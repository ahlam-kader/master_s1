import json
from confluent_kafka import Consumer
import psycopg2

def connect_to_postgresql():
    """Connect to the PostgreSQL database."""
    return psycopg2.connect(
        host="pgdb", 
        database="etldb", 
        user="admin", 
        password="adminx01"
    )

def create_table_if_not_exists():
    """Create the mru_rates table if it doesn't already exist."""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS mru_rates (
        quantite INTEGER,
        devise TEXT,
        code TEXT,
        cours_de_reference NUMERIC,
        date DATE
    );
    """
    conn = connect_to_postgresql()
    try:
        with conn.cursor() as cursor:
            cursor.execute(create_table_query)
            conn.commit()
    finally:
        conn.close()

def save_message_to_postgresql(message):
    """Save a parsed message to the PostgreSQL database."""
    insert_query = """
    INSERT INTO mru_rates (quantite, devise, code, cours_de_reference, date)
    VALUES (%s, %s, %s, %s, %s)
    """
    conn = connect_to_postgresql()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                insert_query,
                (
                    message["quantite"],
                    message["devise"],
                    message["code"],
                    float(message["cours de reference"].replace(",", ".")),
                    message["date"]
                )
            )
            conn.commit()
    finally:
        conn.close()

def send_msg(message):
    create_table_if_not_exists()
    try:
        parsed_message = {
            "quantite": int(message["QUANTITE"]),
            "devise": message["DEVISE"],
            "code": message["CODE"],
            "cours de reference": message["COURS DE REFERENCE"],
            "date": message["Date"]
        }
        save_message_to_postgresql(parsed_message)
        print(f"Saved message: {parsed_message}")
    except Exception as e: print(f"Failed to process message: {message.value} | Error: {e}")


def consume_kafka_messages():

    c = Consumer({
        'bootstrap.servers': 'kafka:9092',
        'group.id': 'bcm-step2',
        'enable.auto.commit': 'false',
        'auto.offset.reset':'earliest'
    })

    c.subscribe(['topic-step1'])
    while True:
        msg = c.poll(0.1)
        if msg is None:
            continue
        elif not msg.error():

            print(msg.value())
            m = json.loads(msg.value().decode('utf-8'))
            send_msg(m)
            
        else:
            print("Error")

if __name__ == "__main__":
    consume_kafka_messages()
