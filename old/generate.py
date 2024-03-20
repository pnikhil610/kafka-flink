from kafka import KafkaProducer
import json
import random
import time
import uuid

# Kafka broker address
bootstrap_servers = 'localhost:9092'
# Kafka topic to push data
topic = 'input_topic'

# Create Kafka producer
producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def generate_dummy_data():
    """Generate dummy data."""
    while True:
        data = {
            "Timestamp": int(time.time() * 1000),  # Current timestamp in milliseconds
            "InstrumentId": "7893841305054116341",
            "Product": "SRAZ24",
            "Price": round(random.uniform(90, 100), 3),  # Random price between 90 and 100
            "Qty": random.randint(1, 100),
            "Direction": random.choice(["Hit", "Miss"]),
            "DirectAskCounterparty": None,
            "DirectBidCounterparty": None,
            "IsImplied": random.choice([True, False]),
            "IsLegTrade": random.choice([True, False]),
            "IsOtc": random.choice([True, False]),
            "OTCTradeType": None,
            "Id": str(uuid.uuid4())
        }
        producer.send(topic, value=data)
        print("Sent:", data)
        time.sleep(0.5)  # Wait for 1 second before sending the next message

if __name__ == "__main__":
    generate_dummy_data()
