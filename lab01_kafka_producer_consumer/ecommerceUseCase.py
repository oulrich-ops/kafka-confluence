import pandas as pd
from confluent_kafka import Producer, Consumer
import time
import json

def convert_csv_to_json():
    csv_file = './data/first_100_customers.csv'
    df = pd.read_csv(csv_file)

    json_records = df.to_dict(orient='records')

    json_file = './data/customers.json'

    with open(json_file, 'w') as f:
        json.dump(json_records, f, indent=4)
        
    print(f"Converted {csv_file} to {json_file}")
    

def read_config():
  config = {}
  with open("client.properties") as fh:
    for line in fh:
      line = line.strip()
      if len(line) != 0 and line[0] != "#":
        parameter, value = line.strip().split('=', 1)
        config[parameter] = value.strip()
  return config

def delivery_status(err, msg):
    if err:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    
def main():
    config = read_config()
    producer = Producer(config)
    with open('./data/customers.json', 'r') as f:
        customer_data = json.load(f)
   
    print(customer_data)
    for record in customer_data:
        print(record)
        key = str(record['customer_id'])
        value = json.dumps(record)
        try:
            producer.produce("ecommerce", key=key, value=value, callback=delivery_status)
            producer.poll(1)
       
        except Exception as e:
            print(f"Failed to produce message: {e}")
        time.sleep(1) 
    producer.flush()
        
if __name__ == "__main__":
    convert_csv_to_json()
    main()