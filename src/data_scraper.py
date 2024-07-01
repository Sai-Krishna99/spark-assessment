import requests
import logging

# Configure logging
logging.basicConfig(filename='logs/data_scraper.log', level=logging.INFO, format='%(asctime)s - %(message)s')

# Download the JSON file
url = "https://raw.githubusercontent.com/kingspp/mlen_assignment/main/data.json"
response = requests.get(url)
#write the data in data folder
with open("data/data.json", "w") as file:
    file.write(response.text)
logging.info("JSON file downloaded successfully.")