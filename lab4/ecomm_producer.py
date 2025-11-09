import pandas as pd
from confluent_kafka import Producer, Consumer
import time
import json
import uuid
import random

products = ["laptop", "smartphone", "tablet", "headphones", "smartwatch",
                "camera", "printer", "monitor", "keyboard", "mouse"]
card_types=["Visa", "MasterCard", "Amex", "Discover"]
country_cities = {
        "USA": ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"],
        "Canada": ["Toronto", "Vancouver", "Montreal", "Calgary", "Ottawa"],
        "UK": ["London", "Manchester", "Birmingham", "Leeds", "Glasgow"],
        "Germany": ["Berlin", "Hamburg", "Munich", "Cologne", "Frankfurt"],
        "France": ["Paris", "Marseille", "Lyon", "Toulouse", "Nice"],
        "Australia": ["Sydney", "Melbourne", "Brisbane", "Perth", "Adelaide"]
    }
    
shopping_websites = ["ShopEasy", "BuyMore", "TechWorld", "GadgetHub", "FashionFiesta"] 

def read_config():
  config = {}
  with open("client.properties") as fh:
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
  


def delivery_status(err, msg):
    if err:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

   
    
def produceEcommeData():
    
    order_id = uuid.uuid4()[:8]
    orderproductname = random.choice(products)
    order_card_types = random.choice(card_types)
    order_amount = round(random.uniform(50.0, 2000.0),
    2)  
    order_date = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
    order_country = random.choice(list(country_cities.keys()))
    order_city = random.choice(country_cities[order_country])
    order_ecommerce_website_name = random.choice(shopping_websites)
    
    topic = "ecommerce"
    config = read_config()
    value = {
        "order_id": str(order_id),
        "product_name": orderproductname,
        "card_type": order_card_types,
        "amount": order_amount,
        "order_date": order_date,
        "country": order_country,
        "city": order_city,
        "ecommerce_website_name": order_ecommerce_website_name
    }
    
    value = json.dumps(value)
    key = str(order_id)
    
    
    produce(topic, config, key=key, value=value)
    time.sleep(1) 


def main():
    for _i in range(100):
        produceEcommeData()
        
        
if __name__ == "__main__":
    main()