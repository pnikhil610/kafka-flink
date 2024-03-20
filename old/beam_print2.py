import apache_beam as beam
from apache_beam.io.kafka import ReadFromKafka
import logging

import logging

def log_element(elem):
    logging.info(elem)
    return elem
   
# Replace these with your details
bootstrap_servers = "localhost:9092"
topic_name = "trade_records"
logging.getLogger().setLevel(logging.INFO)

with beam.Pipeline() as pipeline:
  # Read messages from Kafka
  messages = pipeline | 'ReadFromKafka' >> ReadFromKafka(
      consumer_config={'bootstrap.servers': bootstrap_servers},
      topics=[topic_name])

  # Print each message
  messages | 'PrintMessages' >> beam.Map(log_element)
