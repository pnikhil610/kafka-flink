from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema, JsonRowSerializationSchema
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.functions import ProcessWindowFunction, ProcessAllWindowFunction
from pyflink.datastream import RuntimeContext, WindowedStream
import json
from datetime import datetime
from pyflink.common.time import Time

class OHLCVAggregator(ProcessAllWindowFunction):
    def process(self, context, elements):
        print('process')
        prices = [element[3] for element in elements]  # Assuming the price is the fourth element
        volumes = [element[4] for element in elements]  # Assuming quantity is the fifth element

        open_price = elements[0][3]
        close_price = elements[-1][3]
        high_price = max(prices)
        low_price = min(prices)
        volume = sum(volumes)

        return [(context.window.start, context.window.end, open_price, high_price, low_price, close_price, volume)]

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

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars("file:///home/pnikhil/pyflink/flink-sql-connector-kafka-3.0.2-1.18.jar")
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)

    source_topic = 'source_topic_name'
    target_topic = 'target_topic_name'

    kafka_source = create_kafka_source(env, source_topic)
    data_stream = env.add_source(kafka_source)
    # data_stream.print()

    # Key by product to ensure we calculate OHLCV metrics per product
    keyed_stream = data_stream.key_by(lambda x: x[2])  # Assuming product is the third element

    # Apply windowing
    windowed_stream = keyed_stream \
        .window(TumblingEventTimeWindows.of(Time.seconds(10))) \
        .allowed_lateness(10*1000)

    # Aggregate to OHLCV
    ohlcv_stream = windowed_stream.process(OHLCVAggregator())

    # Serialize and send to Kafka
    ohlcv_stream.map(lambda x: json.dumps({
        "window_start": datetime.fromtimestamp(x[0] // 1000).strftime('%Y-%m-%d %H:%M:%S'),
        "window_end": datetime.fromtimestamp(x[1] // 1000).strftime('%Y-%m-%d %H:%M:%S'),
        "open": x[2],
        "high": x[3],
        "low": x[4],
        "close": x[5],
        "volume": x[6]
    }, ensure_ascii=False)).print() #.add_sink(create_kafka_sink(target_topic))

    env.execute()

if __name__ == "__main__":
    main()
