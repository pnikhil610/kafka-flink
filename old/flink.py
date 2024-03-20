from pyflink.common.serialization import SimpleStringSchema, JsonRowDeserializationSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.table.window import Tumble

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)  # You can adjust the parallelism as needed

# Set up the environment for streaming queries
env_settings = EnvironmentSettings.new_instance().use_blink_planner().build()
t_env = StreamTableEnvironment.create(env, environment_settings=env_settings)

# Kafka connection details
bootstrap_servers = "localhost:9092"
topic_name = "trade_records"

# Define the Kafka consumer
kafka_props = {
    "bootstrap.servers": bootstrap_servers,
    "group.id": "flink-consumer-group"
}
consumer = FlinkKafkaConsumer(
    topic_name,
    JsonRowDeserializationSchema.builder()
        .type_info(Types.ROW([Types.FIELD("timestamp", Types.BIG_INT()),
                              Types.FIELD("instrument_id", Types.STRING()),
                              Types.FIELD("product", Types.STRING()),
                              Types.FIELD("price", Types.DOUBLE()),
                              Types.FIELD("qty", Types.INT())]))
        .build(),
    properties=kafka_props)

# Add the Kafka consumer as a source
trade_stream = t_env.from_source(consumer)

# Define table schema
t_env.create_temporary_view("Trades", trade_stream)

# Define the OHLC calculation query for 1-minute window
ohlc_1min_query = """
    SELECT
        TUMBLE_START(rowtime, INTERVAL '1' MINUTE) AS window_start,
        instrument_id,
        product,
        MAX(price) AS high,
        MIN(price) AS low,
        FIRST_VALUE(price) AS open,
        LAST_VALUE(price) AS close
    FROM Trades
    GROUP BY TUMBLE(rowtime, INTERVAL '1' MINUTE), instrument_id, product
"""

# Define the OHLC calculation query for 5-minute window
ohlc_5min_query = """
    SELECT
        TUMBLE_START(rowtime, INTERVAL '5' MINUTE) AS window_start,
        instrument_id,
        product,
        MAX(price) AS high,
        MIN(price) AS low,
        FIRST_VALUE(price) AS open,
        LAST_VALUE(price) AS close
    FROM Trades
    GROUP BY TUMBLE(rowtime, INTERVAL '5' MINUTE), instrument_id, product
"""

# Register the queries
t_env.register_table("OHLC_1min", t_env.sql_query(ohlc_1min_query))
t_env.register_table("OHLC_5min", t_env.sql_query(ohlc_5min_query))

# Convert the query results into a stream and print
ohlc_1min_stream = t_env.to_append_stream(t_env.table("OHLC_1min"), Types.STRING())
ohlc_5min_stream = t_env.to_append_stream(t_env.table("OHLC_5min"), Types.STRING())

# Print the results
ohlc_1min_stream.print()
ohlc_5min_stream.print()

# Execute the job
env.execute("OHLC Calculation")
