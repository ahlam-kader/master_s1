from confluent_kafka import Consumer
import psycopg2
import json

kafka_consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'match_consumer_group',
    'auto.offset.reset': 'earliest'
})

kafka_consumer.subscribe(['mauritania_league'])

conn = psycopg2.connect(
    dbname="etldb",
    user="admin",
    password="adminx01",
    host="pgdb",
    port=5432
)
cursor = conn.cursor()

cursor.execute("""
    CREATE TABLE IF NOT EXISTS soccer_results (
        id SERIAL PRIMARY KEY,
        game_date DATE,
        home_team TEXT,
        away_team TEXT,
        home_score INT,
        away_score INT,
        home_point INT,
        away_point INT
    );
""")
conn.commit()

def calculate_points(home_score, away_score):
    if home_score > away_score:
        return 3, 0
    elif home_score < away_score:
        return 0, 3
    else:
        return 1, 1

def insert_match_into_postgres():
    for message in kafka_consumer:
        match_data = json.loads(message.value().decode('utf-8'))
        home_point, away_point = calculate_points(match_data["home_score"], match_data["away_score"])

        cursor.execute("""
            INSERT INTO match_results (game_date, home_team, away_team, home_score, away_score, home_point, away_point)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
        """, (match_data["game_date"], match_data["home_team"], match_data["away_team"], match_data["home_score"], match_data["away_score"], home_point, away_point))

        conn.commit()
        print(f"Données insérées : {match_data}")

    conn.close()
    kafka_consumer.close()

if __name__ == "__main__":
    insert_match_into_postgres()
