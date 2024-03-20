#Working example with pushing to kafka every event
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
        self.start_of_minute = None

class OHLCVAggregator(KeyedProcessFunction):
    def __init__(self):
        super().__init__()
        self.state = None

    def open(self, context: RuntimeContext):
        self.state = context.get_state(ValueStateDescriptor("state", Types.PICKLED_BYTE_ARRAY()))

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
        
        # Convert dictionary to JSON string
        json_value = json.dumps(event)

        # Now you can proceed with your processing logic
        # Deserialize and extract necessary info
        event = json.loads(json_value)
        timestamp_ms = event['timestamp']
        price = event['price']
        volume = event['qty']
        
        current_time = datetime.fromtimestamp(timestamp_ms / 1000.0)
        minute_start_time = current_time.replace(second=0, microsecond=0)

        # Load or initialize state
        current_state = self.state.value()
        if current_state is None:
            current_state = ProductState()
            current_state.start_of_minute = minute_start_time
        
        # Check for minute transition
        if current_state.start_of_minute != minute_start_time:
            # Emit end-of-minute OHLCV and reset state
            self.emit_ohlcv(current_state, ctx.timestamp())
            current_state = ProductState()  # Reset state for new minute
            current_state.start_of_minute = minute_start_time
        
        # Update state with current event
        if current_state.open is None:
            current_state.open = price
        current_state.close = price
        current_state.high = max(price, current_state.high)
        current_state.low = min(price, current_state.low)
        current_state.volume += volume

        # Update stored state
        self.state.update(current_state)

        # Optionally, emit current OHLCV state for real-time updates
        self.emit_ohlcv(current_state, ctx.timestamp())

    def emit_ohlcv(self, state, timestamp):
        ohlcv_result = {
            "time": state.start_of_minute.strftime("%Y-%m-%d %H:%M:%S"),
            "open": state.open,
            "high": state.high,
            "low": state.low,
            "close": state.close,
            "volume": state.volume
        }
        # Emit the result for downstream processing
        # This could be to Kafka for real-time updates and/or to a side output for batch processing to PostgreSQL
        print('push this to postgres', json.dumps(ohlcv_result))  # Replace with actual emit logic

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
