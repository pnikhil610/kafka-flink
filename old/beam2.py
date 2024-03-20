import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import json
from datetime import datetime, timedelta
import random

class GenerateMockData(beam.DoFn):
    def process(self, _):
        """Generate mock data similar to what comes from Kafka."""
        for _ in range(1000):  # Generate 1000 mock records
            yield {
                "timestamp": (datetime.now() - timedelta(minutes=random.randint(0, 10))).isoformat(),
                "instrument_id": str(random.randint(1, 10)),  # Mock instrument ID
                "price": random.randint(100, 200),  # Mock price
                "qty": random.randint(1, 10),  # Mock quantity
            }


class CalculateOHLC(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        # Debugging: Inspect window start and end times
        try:
            window_start = window.start.to_utc_datetime()
            window_end = window.end.to_utc_datetime()
        except OverflowError as e:
            print(f"Window start or end time out of range: {e}")
            # Optionally, skip processing this element or handle the error as needed
            return

        # Assuming element[1] is the list of trade dictionaries
        trades = element[1]
        prices = [trade['price'] for trade in trades]
        open_price = trades[0]['price']
        close_price = trades[-1]['price']
        high_price = max(prices)
        low_price = min(prices)

        yield {
            'window_start': window_start.isoformat(),
            'window_end': window_end.isoformat(),
            'open': open_price,
            'high': high_price,
            'low': low_price,
            'close': close_price
        }

def run():
    options = PipelineOptions()

    with beam.Pipeline(options=options) as p:
        (p
         | 'Start' >> beam.Create([None])  # Trigger to start the pipeline
         | 'GenerateMockData' >> beam.ParDo(GenerateMockData())
         | 'MapToTuple' >> beam.Map(lambda record: (record['instrument_id'], record))
         | 'WindowInto' >> beam.WindowInto(beam.window.FixedWindows(60), allowed_lateness=0)
         | 'GroupByInstrumentId' >> beam.GroupByKey()
         | 'CalculateOHLC' >> beam.ParDo(CalculateOHLC())
         | 'PrintOHLC' >> beam.Map(print)
        )

if __name__ == '__main__':
    run()
