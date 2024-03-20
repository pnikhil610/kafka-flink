from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema, JsonRowSerializationSchema
from pyflink.common.typeinfo import Types

def transfer_data_between_topics(env, source_topic, target_topic):
    # Deserialization schema for reading from the original topic
    deserialization_schema = JsonRowDeserializationSchema.Builder() \
        .type_info(Types.ROW([Types.INT(), Types.STRING()])) \
        .build()

    # Consumer configuration for the source topic
    kafka_consumer = FlinkKafkaConsumer(
        topics=source_topic,
        deserialization_schema=deserialization_schema,
        properties={'bootstrap.servers': 'localhost:9092', 'group.id': 'consumer_group'}
    )
    kafka_consumer.set_start_from_earliest()

    # Create a data stream from the source topic
    data_stream = env.add_source(kafka_consumer)

    # Serialization schema for writing to the new topic
    serialization_schema = JsonRowSerializationSchema.Builder() \
        .with_type_info(Types.ROW([Types.INT(), Types.STRING()])) \
        .build()

    # Producer configuration for the target topic
    kafka_producer = FlinkKafkaProducer(
        topic=target_topic,
        serialization_schema=serialization_schema,
        producer_config={'bootstrap.servers': 'localhost:9092'}
    )

    # Add a sink to write the stream to the new topic
    data_stream.add_sink(kafka_producer)

if __name__ == '__main__':
    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars("file:///home/pnikhil/pyflink/flink-sql-connector-kafka-3.0.2-1.18.jar")

    # Specify the source and target topics
    source_topic = 'source_topic_name'
    target_topic = 'target_topic_name'

    # Call the function to transfer data from the source topic to the target topic
    transfer_data_between_topics(env, source_topic, target_topic)

    # Execute the Flink job
    env.execute()
