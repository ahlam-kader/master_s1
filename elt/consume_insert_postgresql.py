import json
from confluent_kafka import Consumer
import psycopg2

POSTGRES_CONFIG = {
    'dbname': 'etldb',
    'user': 'admin',
    'password': 'adminx01',
    'host': 'pgdb',
    'port': 5432
}

KAFKA_CONFIG = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'soccer_results_group',
    'auto.offset.reset': 'earliest'
}
KAFKA_TOPIC = 'soccer_results'

def calculate_points(home_score, away_score):
    if home_score > away_score:
        return (3, 0)  
    elif home_score < away_score:
        return (0, 3)  
    else:
        return (1, 1)  

def insert_into_postgres(cursor, game_data):
    insert_query = """
    INSERT INTO soccer_results (game_date, home_team, away_team, home_score, away_score, home_point, away_point)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    try:
        cursor.execute(
            insert_query,
            (
                game_data['game_date'],  
                game_data['home_team'],
                game_data['away_team'],
                game_data['home_score'],
                game_data['away_score'],
                game_data['home_point'],
                game_data['away_point']
            )
        )
        print(f"Inserted into PostgreSQL: {game_data}")
    except Exception as e:
        print(f"Error inserting data into PostgreSQL: {e}")

def consume_kafka_to_postgres():
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cursor = conn.cursor()
        print("Connected to PostgreSQL")
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")
        return

    consumer = Consumer(KAFKA_CONFIG)
    consumer.subscribe([KAFKA_TOPIC])

    print(f"Subscribed to Kafka topic: {KAFKA_TOPIC}")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            try:
                game_data = json.loads(msg.value().decode('utf-8'))
                print(f"Consumed message: {game_data}")

                if game_data['home_score'] is not None and game_data['away_score'] is not None:
                    home_point, away_point = calculate_points(game_data['home_score'], game_data['away_score'])
                else:
                    print(f"Skipping game with null scores: {game_data}")
                    continue

                game_data['home_point'] = home_point
                game_data['away_point'] = away_point

                insert_into_postgres(cursor, game_data)
                conn.commit()

            except Exception as e:
                print(f"Error processing message: {e}")

    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        consumer.close()
        cursor.close()
        conn.close()
        print("Closed PostgreSQL connection and Kafka consumer")

if __name__ == "__main__":
    consume_kafka_to_postgres()