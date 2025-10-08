from confluent_kafka import Producer, Consumer
import json
import time
import random


def get_current_directory():
  import os
  cwd = os.getcwd()
  print(f"Current working directory: {cwd}")
  return cwd

def read_config():
  # reads the client configuration from client.properties
  # and returns it as a key-value map
  config = {}
  with open("./client.properties") as fh:
    for line in fh:
      line = line.strip()
      if len(line) != 0 and line[0] != "#":
        parameter, value = line.strip().split('=', 1)
        config[parameter] = value.strip()
  return config

def produce(topic, config, key=None, value=None):
  # creates a new producer instance
  producer = Producer(config)
  key = key
  value = value
  producer.produce(topic, key=key, value=value)
  print(f"Produced message to topic {topic}: key = {key:12} value = {value:12}")

  # send any outstanding or buffered messages to the Kafka broker
  producer.flush()
  
def question2Answer(topic, config):
    '''
    simuler l'evolution de la position d'un bus
    en envoyant des mises à jour de position toutes les secondes
    avec des variations aléatoires de la longitude et de la latitude
    '''
    
    bus_id = "bus_123"
    longitude = -122.4194   
    latitude = 37.7749
    for _ in range(100):  
        longitude += random.uniform(-0.001, 0.001)
        latitude += random.uniform(-0.001, 0.001)
        data = {
            "bus_id": bus_id,
            "longitude": longitude,
            "latitude": latitude
        }
        value = json.dumps(data)
        key = bus_id
        print(f"Updated position: {data}")
        produce(topic, config, key=key, value=value)
        time.sleep(1) 
      

#use case https://chatgpt.com/share/68e5a84e-c0b4-800a-ab30-5a07eecf2f8e

def main():
  import os
  cwd = os.getcwd()
  print(f"Current working directory: {cwd}")
  config = read_config()
  topic = "topic_TD01"
  
  '''
   key = "key"
   value = "value"
 '''
 
  '''
  bus_id = "bus_123"
  longitude = -122.4194
  latitude = 37.7749
  data = {
        "bus_id": bus_id,
        "longitude": longitude,
        "latitude": latitude
    }
  value = json.dumps(data)
  key = bus_id  

  produce(topic, config, key=key, value=value)
'''
  question2Answer(topic, config)

main()