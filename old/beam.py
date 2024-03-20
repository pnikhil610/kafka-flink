import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import FixedWindows
from apache_beam.io.kafka import ReadFromKafka
import json
import logging

class CalculateOHLCFn(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        prices = [trade['price'] for trade in element]
        if prices:  # Check if prices list is not empty
            open_price = element[0]['price']
            close_price = element[-1]['price']
            high_price = max(prices)
            low_price = min(prices)

            yield {
                'window_start': window.start.to_utc_datetime().isoformat(),
                'open': open_price,
                'high': high_price,
                'low': low_price,
                'close': close_price
            }

def run():
    options = PipelineOptions(
        runner='FlinkRunner',
        flink_master='localhost:8085',  # Adjust as needed
        # Add any other necessary Flink options here
    )

    p = beam.Pipeline(options=options)

    (p
     | 'ReadFromKafka' >> ReadFromKafka(
         consumer_config={
             'bootstrap.servers': 'localhost:9092',
             'auto.offset.reset': 'earliest'
         },
         topics=['trade_records'],
         key_deserializer='org.apache.kafka.common.serialization.StringDeserializer',
         value_deserializer='org.apache.kafka.common.serialization.StringDeserializer'
     )
     | 'ParseJSON' >> beam.Map(lambda kv: json.loads(kv[1]))  # kv[1] is the message value
     | 'WindowInto' >> beam.WindowInto(FixedWindows(60))  # 1-minute windows
     | 'GroupByWindow' >> beam.GroupByKey()  # Group by window if there's a key; this example doesn't explicitly set one
     | 'CalculateOHLC' >> beam.ParDo(CalculateOHLCFn())
     | 'PrintResults' >> beam.Map(print)
    )

    result = p.run()
    result.wait_until_finish()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
