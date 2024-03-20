from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.table.udf import udf
from pyflink.table import DataTypes
from pyflink.table.descriptors import Schema, Json, Kafka, Jdbc, Rowtime, WatermarkSpec, KafkaValidator, JsonValidator
from pyflink.table.window import Tumble

env = StreamExecutionEnvironment.get_execution_environment()
env_settings = EnvironmentSettings.new_instance().use_blink_planner().build()
t_env = StreamTableEnvironment.create(env, environment_settings=env_settings)

# Define a UDF to parse JSON data
@udf(result_type=DataTypes.ROW(
    [
        DataTypes.FIELD("Timestamp", DataTypes.BIGINT()),
        DataTypes.FIELD("InstrumentId", DataTypes.STRING()),
        DataTypes.FIELD("Product", DataTypes.STRING()),
        DataTypes.FIELD("Price", DataTypes.DOUBLE()),
        DataTypes.FIELD("Qty", DataTypes.INT()),
        DataTypes.FIELD("Direction", DataTypes.STRING()),
        DataTypes.FIELD("DirectAskCounterparty", DataTypes.STRING()),
        DataTypes.FIELD("DirectBidCounterparty", DataTypes.STRING()),
        DataTypes.FIELD("IsImplied", DataTypes.BOOLEAN()),
        DataTypes.FIELD("IsLegTrade", DataTypes.BOOLEAN()),
        DataTypes.FIELD("IsOtc", DataTypes.BOOLEAN()),
        DataTypes.FIELD("OTCTradeType", DataTypes.STRING()),
        DataTypes.FIELD("Id", DataTypes.STRING())
    ]
))
def parse_json(row: str):
    import json
    return json.loads(row)

# Define a UDF to calculate OHLC candles
@udf(result_type=DataTypes.ROW(
    [
        DataTypes.FIELD("open", DataTypes.DOUBLE()),
        DataTypes.FIELD("high", DataTypes.DOUBLE()),
        DataTypes.FIELD("low", DataTypes.DOUBLE()),
        DataTypes.FIELD("close", DataTypes.DOUBLE())
    ]
))
def calculate_ohlc(window_data: list):
    prices = [row["Price"] for row in window_data]
    return {
        "open": prices[0],
        "high": max(prices),
        "low": min(prices),
        "close": prices[-1]
    }

# Define Kafka source and sink properties
kafka_props = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "ohlc-consumer-group",
    "auto.offset.reset": "latest"
}

# Define Kafka source table
t_env.connect(
    Kafka()
    .version("universal")
    .topic("input_topic")
    .start_from_latest()
    .properties(kafka_props)
) \
    .with_format(
    Json()
    .json_schema(
    """
    {
        "type": "object",
        "properties": {
            "Timestamp": {"type": "integer"},
            "InstrumentId": {"type": "string"},
            "Product": {"type": "string"},
            "Price": {"type": "number"},
            "Qty": {"type": "integer"},
            "Direction": {"type": "string"},
            "DirectAskCounterparty": {"type": "string"},
            "DirectBidCounterparty": {"type": "string"},
            "IsImplied": {"type": "boolean"},
            "IsLegTrade": {"type": "boolean"},
            "IsOtc": {"type": "boolean"},
            "OTCTradeType": {"type": "string"},
            "Id": {"type": "string"}
        }
    }
    """
    )
    .fail_on_missing_field(True)
) \
    .with_schema(
    Schema()
    .field("Timestamp", DataTypes.BIGINT())
    .field("InstrumentId", DataTypes.STRING())
    .field("Product", DataTypes.STRING())
    .field("Price", DataTypes.DOUBLE())
    .field("Qty", DataTypes.INT())
    .field("Direction", DataTypes.STRING())
    .field("DirectAskCounterparty", DataTypes.STRING())
    .field("DirectBidCounterparty", DataTypes.STRING())
    .field("IsImplied", DataTypes.BOOLEAN())
    .field("IsLegTrade", DataTypes.BOOLEAN())
    .field("IsOtc", DataTypes.BOOLEAN())
    .field("OTCTradeType", DataTypes.STRING())
    .field("Id", DataTypes.STRING())
    .rowtime(
    Rowtime()
    .timestamps_from_field("Timestamp")
    .watermarks_periodic_bounded(60000)
    )
) \
    .create_temporary_table("kafka_source")

# Parse JSON data
t_env.register_function("parse_json", parse_json)

# Calculate OHLC candles
t_env.register_function("calculate_ohlc", calculate_ohlc)

# Define streaming SQL query for 1-minute window
t_env.sql_update("""
    CREATE VIEW ohlc_1m AS
    SELECT
        TUMBLE_START(Timestamp, INTERVAL '1' MINUTE) as window_start,
        calculate_ohlc(parse_json(data)) as ohlc
    FROM
        kafka_source
    GROUP BY
        TUMBLE(Timestamp, INTERVAL '1' MINUTE)
""")

# Define Kafka sink table for OHLC candles
t_env.connect(
    Kafka()
    .version("universal")
    .topic("output_topic")
    .properties(kafka_props)
) \
    .with_format(
    Json()
    .json_schema(
    """
    {
        "type": "object",
        "properties": {
            "window_start": {"type": "integer"},
            "ohlc": {
                "type": "object",
                "properties": {
                    "open": {"type": "number"},
                    "high": {"type": "number"},
                    "low": {"type": "number"},
                    "close": {"type": "number"}
                }
            }
        }
    }
    """
    )
) \
    .with_schema(
    Schema()
    .field("window_start", DataTypes.BIGINT())
    .field("ohlc", DataTypes.ROW(
    [
        DataTypes.FIELD("open", DataTypes.DOUBLE()),
        DataTypes.FIELD("high", DataTypes.DOUBLE()),
        DataTypes.FIELD("low", DataTypes.DOUBLE()),
        DataTypes.FIELD("close", DataTypes.DOUBLE())
    ]
    ))
) \
    .in_append_mode() \
    .create_temporary_table("kafka_sink")

# Sink OHLC candles to Kafka
t_env.sql_update("""
    INSERT INTO kafka_sink
    SELECT window_start, ohlc
    FROM ohlc_1m
""")

# # Define JDBC sink properties
# jdbc_props = {
#     "url": "jdbc:mysql://localhost:3306/your_database",
#     "username": "your_username",
#     "password": "your_password",
#     "table-name": "ohlc_minute"
# }
#
# # Define JDBC sink table for minute-wise OHLC data
# t_env.connect(
#     Jdbc()
#     .url(jdbc_props["url"])
#     .username(jdbc_props["username"])
#     .password(jdbc_props["password"])
# ) \
#     .with_schema(
#     Schema()
#     .field("window_start", DataTypes.BIGINT())
#     .field("open", DataTypes.DOUBLE())
#     .field("high", DataTypes.DOUBLE())
#     .field("low", DataTypes.DOUBLE())
#     .field("close", DataTypes.DOUBLE())
