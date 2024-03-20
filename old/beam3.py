
from apache_beam.io.kafka import ReadFromKafka
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

class ParseJsonFn(beam.DoFn):
    def process(self, element):
        # Assuming the Kafka message value is a UTF-8 encoded string
        message = element[1].decode('utf-8')  # element[1] is the message value as bytes
        yield json.loads(message)

def run():
    options = PipelineOptions(
        runner='FlinkRunner',
        flink_master='localhost:8085',  # Adjust as needed
    )

    with beam.Pipeline(options=options) as p:
        (p
         | 'ReadFromKafka' >> ReadFromKafka(
             consumer_config={'bootstrap.servers': 'localhost:9092', 'auto.offset.reset': 'earliest'},
             topics=['trade_records'],
             key_deserializer='org.apache.kafka.common.serialization.ByteArrayDeserializer',
             value_deserializer='org.apache.kafka.common.serialization.ByteArrayDeserializer'
         )
         | 'ParseJson' >> beam.ParDo(ParseJsonFn())
         | 'Print' >> beam.Map(print)
        )

if __name__ == '__main__':
    run()
