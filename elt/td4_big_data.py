import pandas as pd
from confluent_kafka import Producer
import time

KAFKA_SERVER = "10.136.127.1:9092"
KAFKA_TOPIC = "big_data_td4_21012"

KAFKA_CONFIG = {
    'bootstrap.servers': KAFKA_SERVER,
    'client.id': 'match_producer'
}
producer = Producer(KAFKA_CONFIG)

file_path = "election2024.short.csv"
df = pd.read_csv(file_path, header=None)

print("Aperçu des données :")
print(df.head())

if df.shape[1] < 5:
    raise ValueError("Le fichier CSV doit contenir au moins 5 colonnes.")

def send_vote(wilaya, moughataa, commune, candidate, nb_votes):
    message = f"{wilaya},{moughataa},{commune},{candidate},{nb_votes}"
    producer.produce(KAFKA_TOPIC, value=message.encode('utf-8'))
    producer.flush()
    print(f"Envoyé: {message}")

df_selected = pd.concat([df.iloc[3:10]])

for _, row in df_selected.iterrows():
    send_vote(row[0], row[1], row[2], row[3], row[4])
    time.sleep(0.5)

print(f"{len(df_selected)} messages ont été envoyés à Kafka !")
