import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.kafka import ReadFromKafka
import json

class ParseRecordFn(beam.DoFn):
    def process(self, element):
        """Parse the Kafka record into a dictionary."""
        print(element)
        return [json.loads(element[1])]

class CalculateOHLC(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        prices = [e['price'] for e in element]
        open_price = element[0]['price']
        close_price = element[-1]['price']
        high_price = max(prices)
        low_price = min(prices)

        yield {
            'window': window.start.to_utc_datetime().isoformat(),
            'open': open_price,
            'high': high_price,
            'low': low_price,
            'close': close_price
        }

def run():
    options = PipelineOptions()

    with beam.Pipeline(options=options) as p:
        (p
         | 'ReadFromKafka' >> ReadFromKafka(consumer_config={'bootstrap.servers': 'localhost:9092'},
                                             topics=['trade_records'])
         | 'ParseRecord' >> beam.ParDo(ParseRecordFn())
         | 'WindowInto' >> beam.WindowInto(beam.window.FixedWindows(60))  # 1-minute window
         | 'GroupByWindow' >> beam.GroupByKey()  # Assuming a key has been assigned prior to windowing
         | 'CalculateOHLC' >> beam.ParDo(CalculateOHLC())
         | 'PrintOHLC' >> beam.Map(print)  # This is for demonstration. Replace as needed.
        )

        # For a 5-minute window, duplicate the relevant steps with `beam.WindowInto(beam.window.FixedWindows(300))`

if __name__ == '__main__':
    run()
