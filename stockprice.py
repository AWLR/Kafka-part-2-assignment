import json
import random
import time
from confluent_kafka import Producer

def generate_stock_update():
    stock_symbol = "STK" + str(random.randint(1, 100))
    current_price = round(random.uniform(100, 1000), 2)
    timestamp = int(time.time())
    return {"symbol": stock_symbol, "price": current_price, "timestamp": timestamp}

producer = Producer({"bootstrap.servers": "localhost:9092"})

while True:
    stock_update = generate_stock_update()
    producer.produce("stock_prices", value=json.dumps(stock_update))
    time.sleep(1)