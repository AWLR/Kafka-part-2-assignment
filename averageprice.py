import json
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.streams import StreamsBuilder, WindowedSerde, WindowedAvg
from confluent_kafka.streams.windows import TimeWindows

def average_price_callback(error, message):
    if error is not None:
        print("Error: {}".format(error))
    else:
        stock_update = json.loads(message.value())
        stock_symbol = stock_update["symbol"]
        current_price = stock_update["price"]
        timestamp = stock_update["timestamp"]

        # Update the average price for the stock symbol
        average_price = average_prices.get(stock_symbol)
        if average_price is None:
            average_prices[stock_symbol] = current_price
        else:
            average_prices[stock_symbol] = (average_price + current_price) / 2

        # Log the current average price for the stock symbol
        print("Average price for stock {}: {}".format(stock_symbol, average_prices[stock_symbol]))

builder = StreamsBuilder()

# Define the state store for average prices
average_prices = builder.add_state_store("average-price-store", WindowedSerde(str, float), TimeWindows(60000))

# Process the stream and update the average price for each stock symbol
builder.stream("stock_prices").foreach(average_price_callback, state_store=average_prices)

# Build the topology and start the consumer
topology = builder.build()

consumer = Consumer({"bootstrap.servers": "localhost:9092", "group.id": "average-price-group"})

consumer.subscribe