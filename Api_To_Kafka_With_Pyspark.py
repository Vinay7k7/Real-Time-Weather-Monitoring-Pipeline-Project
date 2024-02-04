""" I Have created this code for the alternative solution for "Api_To_Kafka.py"
        if u really need to use the Pyspark.
        in this space also !

        
            🦉 Created By :- Vinay Kumar Karivena.
            🧨 Date       :-04-02-2024.
            🍔 Use        :- For Mini Project in Cognizant

"""


from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, udf
from pyspark.sql.types import StringType
import json
import requests
from kafka import KafkaProducer
import time

# Kafka configuration
kafka_bootstrap_servers = 'localhost:9092' # default server address for localhost💀
kafka_topic = 'weather' # you can chang the topic for you use !

# List of cities
cities = ['Hyderabad', 'Delhi', 'Chennai', 'Kolkata', 'Bangalore'] # you can alter the list of cities as per your requirement

# Base URL for OpenWeatherMap API
base_url = "http://api.openweathermap.org/data/2.5/weather" # This is my base URL for OpenWeatherMap API

# API key
api_key = "70fb0a757c2bfa230e8f00cce4e655b" #This is where you have to replace your key !

# Function to fetch weather data for list of city's data from the api endpoint !
def get_weather_data(city):
    params = {
        'q': city,
        'appid': api_key,
        'units': 'metric'
    }
    try:
        response = requests.get(base_url, params=params)
        data = response.json()
        if response.status_code == 200:
            return json.dumps(data)  # Serialize data to JSON string
        else:
            print("Failed to fetch data for {}. Status code: {}".format(city, response.status_code))
            return None
    except requests.RequestException as e:
        print("Request Exception: {}".format(e))
        return None

# Initialize Kafka producer to push the city data that we get from the endpoint to the kafka topic
producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers, value_serializer=lambda v: str(v).encode('utf-8'))

# Create a Spark Session for the real time data flow !
spark = SparkSession.builder.appName("WeatherDataToKafka").getOrCreate()

# Define a UDF to fetch weather data for of  each city in the list and create DataFrame with columns 
def fetch_weather_data(city):
    return get_weather_data(city)

fetch_weather_data_udf = udf(fetch_weather_data, StringType())

# Main loop to send data every 20 seconds
while True:
    # Iterate over cities and send data to Kafka
    for city in cities:
        # Fetch weather data for each city
        weather_data = get_weather_data(city)
        if weather_data:
            # Send weather data to Kafka
            producer.send(kafka_topic, value=weather_data)
            print("Weather data for {} pushed to Kafka !".format(city))

    # Flush the producer to ensure all messages are sent 🤢
    producer.flush()

    # Sleep for 20 seconds you can change the sleep time if u want to!😵‍💫
    time.sleep(20)

# Close the Kafka producer (this will not be reached in an infinite loop) in your dreams !🤣
producer.close()
