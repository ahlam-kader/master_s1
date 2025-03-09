import requests
from bs4 import BeautifulSoup
from confluent_kafka import Producer
import json

KAFKA_CONFIG = {
    'bootstrap.servers': 'kafka:9092',
    'client.id': 'match_producer'
}

producer = Producer(KAFKA_CONFIG)
KAFKA_TOPIC = "soccer_results"

URL = "https://footystats.org/mauritania/super-d1/fixtures"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

def fetch_html():
    try:
        response = requests.get(URL, headers=HEADERS, timeout=10)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        print(f"Erreur lors de la récupération du site : {e}")
        return None

def parse_match_data(html):
    soup = BeautifulSoup(html, "html.parser")
    match_containers = soup.find_all('div', class_='full-matches-table mt1e')

    if not match_containers:
        print("Aucune donnée de match trouvée. Vérifiez la structure HTML.")
        return []

    match_list = []
    for container in match_containers:
        match_items = container.find_all('ul', class_='match')
        for match in match_items:
            try:
                game_date_element = match.find('li', class_='date').find('div', class_='used-to-be-a')
                home_team_element = match.find('a', class_='home').find('span', class_='hover-modal-parent')
                away_team_element = match.find('a', class_='away').find('span', class_='hover-modal-parent')
                score_element = match.find('a', class_='h2h-link pr fl').find("span", class_="ft-score")

                game_date = game_date_element.text.strip() if game_date_element else "N/A"
                home_team = home_team_element.text.strip() if home_team_element else "N/A"
                away_team = away_team_element.text.strip() if away_team_element else "N/A"

                if score_element:
                    scores = score_element.text.strip().split(' - ')
                    home_score = int(scores[0]) if scores[0].isdigit() else None
                    away_score = int(scores[1]) if scores[1].isdigit() else None
                else:
                    home_score = away_score = None

                match_data = {
                    "game_date": game_date,
                    "home_team": home_team,
                    "away_team": away_team,
                    "home_score": home_score,
                    "away_score": away_score
                }
                match_list.append(match_data)

            except Exception as e:
                print(f"Erreur lors de l'extraction d'un match : {e}")

    return match_list

def send_to_kafka(matches):
    if not matches:
        print("Aucun match à envoyer à Kafka.")
        return
    
    for match in matches:
        try:
            producer.produce(KAFKA_TOPIC, key=match["game_date"], value=json.dumps(match))
            producer.flush()
            print(f"Match envoyé à Kafka : {match}")
        except Exception as e:
            print(f"Erreur d'envoi Kafka : {e}")

def main():
    html = fetch_html()
    if not html:
        print("Impossible de récupérer la page, arrêt du script.")
        return

    matches = parse_match_data(html)
    if matches:
        print(f"{len(matches)} matchs récupérés.")
        send_to_kafka(matches)
    else:
        print("Aucune donnée de match disponible.")

if __name__ == "__main__":
    main()
