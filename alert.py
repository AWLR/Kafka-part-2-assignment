import json
from confluent_kafka import Consumer, KafkaError

def alert_callback(error, message):
    if error is not None:
        print("Error: {}".format(error))
    else:
        stock_update = json.loads(message.value())
        stock_symbol = stock_update["symbol"]
        current_price = stock_update["price"]
        timestamp = stock_update["timestamp"]

        if previous_stock_update is not None:
            price_change_percentage = (current_price - previous_stock_update["price"]) / previous_stock_update["price"] * 100

            if abs(price_change_percentage) > 5:
                print("ALERT: Significant price change detected for stock {}: {}%".format(stock_symbol, round(price_change_percentage, 2)))

        previous_stock_update = stock_update

consumer = Consumer({"bootstrap.servers": "localhost:9092", "group.id": "alert-group"})

consumer.subscribe(["stock_prices"])

previous_stock_update = None

while True:
    message = consumer.poll(1.0)

    if message is None:
        continue
    if message.error():
        print("Consumer error: {}".format(message.error()))
    else:
        alert_callback(None, message)