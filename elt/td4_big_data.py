# import pandas as pd
# from confluent_kafka import Producer
# import time

# # Configuration Kafka
# KAFKA_SERVER = "10.136.127.1:9092"
# KAFKA_TOPIC = "big_data_td4_21012"

# KAFKA_CONFIG = {
#     'bootstrap.servers': KAFKA_SERVER,
#     'client.id': 'match_producer'
# }
# producer = Producer(KAFKA_CONFIG)

# # Lire le fichier CSV sans colonnes
# file_path = "election2024.short.csv"
# df = pd.read_csv(file_path, header=None)  # header=None signifie "pas de noms de colonnes"

# # Afficher les 5 premières lignes pour vérification
# print("Aperçu des données :")
# print(df.head())

# # Vérifier que le fichier a au moins 5 colonnes
# if df.shape[1] < 5:
#     raise ValueError("Le fichier CSV doit contenir au moins 5 colonnes.")

# # Fonction d'envoi des messages à Kafka
# def send_vote(wilaya, moughataa, commune, candidate, nb_votes):
#     message = f"{wilaya},{moughataa},{commune},{candidate},{nb_votes}"
#     producer.produce(KAFKA_TOPIC, value=message.encode('utf-8'))
#     producer.flush()
#     print(f"Envoyé: {message}")

# # Envoyer les lignes à Kafka
# for _, row in df.iterrows():
#     send_vote(row[0], row[1], row[2], row[3], row[4])  # Accès par index
#     time.sleep(0.5)  # Pause pour éviter la surcharge

# print("Tous les messages ont été envoyés à Kafka !")





import pandas as pd
from confluent_kafka import Producer
import time

# Configuration Kafka
KAFKA_SERVER = "10.136.127.1:9092"
KAFKA_TOPIC = "big_data_td4_21012"

KAFKA_CONFIG = {
    'bootstrap.servers': KAFKA_SERVER,
    'client.id': 'match_producer'
}
producer = Producer(KAFKA_CONFIG)

# Lire le fichier CSV sans colonnes
file_path = "election2024.short.csv"
df = pd.read_csv(file_path, header=None)

# Afficher les 5 premières lignes pour vérification
print("Aperçu des données :")
print(df.head())

# Vérifier qu'il y a assez de colonnes
if df.shape[1] < 5:
    raise ValueError("Le fichier CSV doit contenir au moins 5 colonnes.")

# Fonction d'envoi des messages à Kafka
def send_vote(wilaya, moughataa, commune, candidate, nb_votes):
    message = f"{wilaya},{moughataa},{commune},{candidate},{nb_votes}"
    producer.produce(KAFKA_TOPIC, value=message.encode('utf-8'))
    producer.flush()
    print(f"Envoyé: {message}")

# Sélectionner les lignes spécifiques : de 1 à 25 et de 30 à 45
# df_selected = pd.concat([df.iloc[0:25], df.iloc[30:45]])  # Attention : index commence à 0 !
df_selected = pd.concat([df.iloc[3:10]])

# Envoyer les lignes sélectionnées à Kafka
for _, row in df_selected.iterrows():
    send_vote(row[0], row[1], row[2], row[3], row[4])
    time.sleep(0.5)

print(f"{len(df_selected)} messages ont été envoyés à Kafka !")
