from confluent_kafka import Producer
import requests, re, json
from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd

def scrape_bcm_exchange_rates():
    url = "https://www.bcm.mr/cours-de-change.html"
   
    print("Fetching webpage...")
    # Perform the request with SSL verification disabled
    response = requests.get(url, verify=False)
   
    if response.status_code != 200:
        raise Exception(f"Failed to fetch the webpage. Status code: {response.status_code}")
   
    print("Parsing HTML...")
    soup = BeautifulSoup(response.content, 'html.parser')
   
    # Find the specific table for "Cours de Reference"
    table_div = soup.find("div", class_="rTable")
    if not table_div:
        raise Exception("Table not found in the page!")
   
    # Extract headers
    headers = [header.text.strip() for header in table_div.find("div", class_="rTableRow").find_all("div")]
    headers.append('Date')
    # Extract the date
    # date_div = soup.find("div", class_="rTable")
    date_div = soup.select_one('div.templatemo_box > h2').get_text()
    pattern = r"\d{2}/\d{2}/\d{4}"
    match = re.search(pattern, date_div)
    if match:
        dd=match.group()
    else:
        today = datetime.today()
        dd = today.strftime("%d/%m/%Y")

    # Extract data rows
    rows = []
    for row in table_div.find_all("div", class_="rTableRow")[1:]:  # Skip header row
        cells = [cell.text.strip() for cell in row.find_all("div")]
        cells.append(dd)
        rows.append(cells)
   
    # Create a DataFrame for better visualization
    df = pd.DataFrame(rows, columns=headers)
    return df

def send_to_kafka(topic, data):
    p = Producer({'bootstrap.servers': 'kafka:9092'})
    #msg = data.to_json(orient='records')
    p.produce(topic, value=data)
    p.flush()

if __name__ == "__main__":
    try:
        exchange_rates_df = scrape_bcm_exchange_rates()
        print("Exchange Rates fetched!")
        for x in exchange_rates_df.to_dict(orient='records'):
            send_to_kafka('topic-step1', json.dumps(x))
        
        #send_to_kafka('topic-step1', exchange_rates_df)
        print("Exchange rates saved to kafka topic 'topic-step1'")
    except Exception as e:
        print(f"Error: {e}")

