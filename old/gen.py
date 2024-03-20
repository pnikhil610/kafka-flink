import time
import json
import random
from kafka import KafkaProducer
from datetime import datetime, timezone

# Initialize a Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],  # Update to match your Kafka broker(s)
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

products = [f"Product_{i}" for i in range(1, 2000)]  # Generating 20 product names

def generate_data():
    """Generates a dict with random data for a product."""
    utc_now = datetime.now(timezone.utc)
    unix_timestamp = int(utc_now.timestamp()*1000)

    return {
        "timestamp": unix_timestamp,
        "instrument_id": f"Instrument_{random.randint(1, 100)}",
        "product": random.choice(products),
        "price": random.randint(1, 1000),
        "qty": random.randint(1, 100)
    }

k = 0
try:
    while True:
        data = generate_data()
        producer.send('source_topic_name', value=data)
        # producer.flush()
        k += 1
        print(f"Data sent: {data} {k}")
        time.sleep(0.0005)  # Wait for 1 second before sending the next message
except KeyboardInterrupt:
    print("\nStopped.")
finally:
    producer.close()
