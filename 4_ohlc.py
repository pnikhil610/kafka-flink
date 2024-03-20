from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.datastream.window import TumblingEventTimeWindows, TumblingProcessingTimeWindows
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema, JsonRowSerializationSchema
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.functions import ProcessWindowFunction, ProcessAllWindowFunction
from pyflink.common import Duration
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.functions import MapFunction, KeyedProcessFunction, RuntimeContext
from pyflink.common.watermark_strategy import TimestampAssigner
import json
from datetime import datetime
from pyflink.common.time import Time

class ProductState:
    def __init__(self):
        self.high = None
        self.low = None
        self.open = None
        self.close = None
        self.volume = 0

class OHLCVAggregator(KeyedProcessFunction):
    def __init__(self):
        self.state = None

    def open(self, context: RuntimeContext):
        self.state = context.get_state(ValueStateDescriptor("state", Types.PICKLED_BYTE_ARRAY()))

    def process_element(self, value, ctx):
        # Assuming value is a dictionary with needed fields
        current_time = datetime.fromtimestamp(value['timestamp'] / 1000.0)
        price = value['price']
        volume = value['qty']
        
        # Initialize or update state
        current_state = self.state.value() or ProductState()
        if current_state.open is None:
            current_state.open = price
        current_state.close = price
        current_state.high = max(price, current_state.high or price)
        current_state.low = min(price, current_state.low or price)
        current_state.volume += volume

        # Emit current OHLCV values
        result = {
            "time": current_time.strftime("%Y-%m-%d %H:%M:%S"),
            "open": current_state.open,
            "high": current_state.high,
            "low": current_state.low,
            "close": current_state.close,
            "volume": current_state.volume
        }
        yield (value['product'], json.dumps(result))

        # Update state
        self.state.update(current_state)

def create_kafka_source(env, topic):
    row_type_info = Types.ROW_NAMED(['timestamp', 'instrument_id', 'product', 'price', 'qty'], [Types.LONG(), Types.STRING(), Types.STRING(), Types.INT(), Types.INT()])
    deserialization_schema = JsonRowDeserializationSchema.Builder() \
        .type_info(row_type_info).build()

    return FlinkKafkaConsumer(
        topics=topic,
        deserialization_schema=deserialization_schema,
        properties={'bootstrap.servers': 'localhost:9092', 'group.id': 'flink_consumer_group'}
    )

def create_kafka_sink(topic):
    return FlinkKafkaProducer(
        topic=topic,
        serialization_schema=SimpleStringSchema(),
        producer_config={'bootstrap.servers': 'localhost:9092'}
    )


class MyTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        # Extract the timestamp from your event. Assuming 'timestamp' is the field and in milliseconds.
        return value['timestamp']

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars("file:///home/pnikhil/pyflink/flink-sql-connector-kafka-3.0.2-1.18.jar")
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)

    source_topic = 'source_topic_name'
    target_topic = 'target_topic_name'

    kafka_source = create_kafka_source(env, source_topic)
     # Define the WatermarkStrategy
    # watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(5)) \
    #                                        .with_timestamp_assigner(MyTimestampAssigner())
    watermark_strategy = WatermarkStrategy.for_monotonous_timestamps() \
        .with_timestamp_assigner(MyTimestampAssigner())
    data_stream = env.add_source(kafka_source).assign_timestamps_and_watermarks(watermark_strategy)
    # data_stream.print()

    # Key by product to ensure we calculate OHLCV metrics per product
    keyed_stream = data_stream.key_by(lambda x: x[2])  # Assuming product is the third element

    # Apply windowing
    windowed_stream = keyed_stream \
        # .window(TumblingEventTimeWindows.of(Time.seconds(10))) \
        # .allowed_lateness(10*1000)

    # Aggregate to OHLCV
    ohlcv_stream = windowed_stream.process(OHLCVAggregator())

    # Serialize and send to Kafka
    ohlcv_stream.print()
    # ohlcv_stream.map(lambda x: json.dumps({
    #     "window_start": datetime.fromtimestamp(x[0] // 1000).strftime('%Y-%m-%d %H:%M:%S'),
    #     "window_end": datetime.fromtimestamp(x[1] // 1000).strftime('%Y-%m-%d %H:%M:%S'),
    #     "open": x[2],
    #     "high": x[3],
    #     "low": x[4],
    #     "close": x[5],
    #     "volume": x[6]
    # }, ensure_ascii=False)).print() #.add_sink(create_kafka_sink(target_topic))

    env.execute()

if __name__ == "__main__":
    main()
