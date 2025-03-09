from confluent_kafka import Producer
import json
from extract_game_results import match_results

producer = Producer({'bootstrap.servers': 'localhost:9092'})

def send_results_to_kafka():
    match_results = match_results()

    for match in match_results:
        producer.produce('mauritania_league', value=json.dumps(match))
        print(f"Match envoyé à Kafka : {match}")

    producer.flush()

if __name__ == "__main__":
    send_results_to_kafka()
