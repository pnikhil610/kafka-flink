from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.kafka import ReadFromKafka
import apache_beam as beam
import os
import logging

logging.getLogger().setLevel(logging.INFO)
def print_message(element):
    os.system("touch 'test'")
    return elementlogging.getLogger().setLevel(logging.INFO)

def run():
    options = PipelineOptions()

    with beam.Pipeline(options=options) as p:
        (p
         | 'ReadFromKafka' >> ReadFromKafka(consumer_config={
                 'bootstrap.servers': 'localhost:9092',
                 'auto.offset.reset': 'earliest'  # Start reading from the beginning of the topic
             },
             topics=['trade_records'])
         | 'PrintRawMessages' >> beam.Map(print_message)
        )

if __name__ == '__main__':
    run()
