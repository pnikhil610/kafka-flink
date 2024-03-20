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
from datetime import datetime, timedelta
from pyflink.common.time import Time



import json
from datetime import datetime
from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor

class ProductState:
    def __init__(self):
        self.high = float('-inf')
        self.low = float('inf')
        self.open = None
        self.close = None
        self.volume = 0
        self.start_of_window = None

class OHLCVAggregator(KeyedProcessFunction):
    def __init__(self, window_interval):
        super().__init__()
        self.window_interval = window_interval
        self.state = None
        self.kafka_producer = None

    def open(self, context: RuntimeContext):
        self.state = context.get_state(ValueStateDescriptor("state", Types.PICKLED_BYTE_ARRAY()))
        print('initializing kafka')
        # Initialize Kafka producer
        # self.kafka_producer = FlinkKafkaProducer(
        #     topic="ohlcv_topic",
        #     serialization_schema=SimpleStringSchema(),
        #     producer_config={'bootstrap.servers': 'localhost:9092'}
        # )

    def process_element(self, value, ctx):
        # Extract fields from Row object
        timestamp = value[0]
        instrument_id = value[1]
        product = value[2]
        price = value[3]
        qty = value[4]
        
        # Construct a dictionary
        event = {
            "timestamp": timestamp,
            "instrument_id": instrument_id,
            "product": product,
            "price": price,
            "qty": qty
        }
        
        current_time = datetime.fromtimestamp(timestamp / 1000.0)

        # Load or initialize state
        current_state = self.state.value()
        if current_state is None:
            current_state = ProductState()
            current_state.start_of_window = current_time.replace(second=0, microsecond=0)

        # Check for window transition
        if current_time - current_state.start_of_window >= timedelta(minutes=self.window_interval):
            # Emit end-of-window OHLCV and reset state
            self.emit_ohlcv(current_state, ctx.timestamp(), product)
            current_state = ProductState()  # Reset state for new window
            current_state.start_of_window = current_time.replace(second=0, microsecond=0)
        
        # Update state with current event
        if current_state.open is None:
            current_state.open = price
        current_state.close = price
        current_state.high = max(price, current_state.high)
        current_state.low = min(price, current_state.low)
        current_state.volume += volume

        # Update stored state
        self.state.update(current_state)

        # Emit current OHLCV state for real-time updates
        self.emit_ohlcv(current_state, ctx.timestamp(), product)

    def on_timer(self, timestamp, ctx):
        # Emit the final window OHLCV to the database
        current_state = self.state.value()
        if current_state is not None:
            self.emit_ohlcv(current_state, ctx.timestamp(), product)
            print('pushing to db', current_state)

    def emit_ohlcv(self, state, timestamp, product):
        ohlcv_result = {
            "time": state.start_of_window.strftime("%Y-%m-%d %H:%M:%S"),
            "product": product,
            "open": state.open,
            "high": state.high,
            "low": state.low,
            "close": state.close,
            "volume": state.volume
        }
        # Emit the result to Kafka for real-time updates
        print('pushing to kafka', ohlcv_result)
        # self.kafka_producer.produce(json.dumps(ohlcv_result))
#
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
    ohlcv_stream = windowed_stream.process(OHLCVAggregator(1))

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
