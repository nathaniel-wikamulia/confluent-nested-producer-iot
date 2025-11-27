from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

kafka_conf = {
    'bootstrap.servers': '<CLOUD CLUSTER BOOTSTRAP SERVER>',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': '<CLOUD CLUSTER API KEY>',
    'sasl.password': '<CLOUD CLUSTER API SECRET>',
}

schema_registry_conf = {
    'url': '<SCHEMA REGISTRY PUBLIC ENDPOINT>',
    'basic.auth.user.info': '<SCHEMA REGISTRY API KEY>:<SCHEMA REGISTRY API SECRET>',
}

key_schema = """
{
  "type": "record",
  "name": "DeviceKey",
  "namespace": "com.example.metrics",
  "fields": [
    { "name": "M1kwh", "type": { "type": "array", "items": "double" } }
  ]
}
"""

# Avro (not JSON Schema) describing your payload
value_schema = """
{
  "type": "record",
  "name": "DeviceMetricsWrapper",
  "namespace": "com.example.metrics",
  "fields": [
    {
      "name": "d",
      "type": {
        "type": "record",
        "name": "DeviceMetrics",
        "fields": [
          { "name": "M1kwh", "type": { "type": "array", "items": "double" } },
          { "name": "M1kw",  "type": { "type": "array", "items": "double" } },
          { "name": "M1v",   "type": { "type": "array", "items": "double" } },
          { "name": "M1a",   "type": { "type": "array", "items": "double" } },
          { "name": "M1ar",  "type": { "type": "array", "items": "double" } },
          { "name": "M1as",  "type": { "type": "array", "items": "double" } },
          { "name": "M1at",  "type": { "type": "array", "items": "double" } },
          { "name": "M1vz",  "type": { "type": "array", "items": "double" } },
          { "name": "M1vx",  "type": { "type": "array", "items": "double" } },
          { "name": "M1tmp", "type": { "type": "array", "items": "double" } },

          { "name": "M2kwh", "type": { "type": "array", "items": "double" } },
          { "name": "M2kw",  "type": { "type": "array", "items": "double" } },
          { "name": "M2v",   "type": { "type": "array", "items": "double" } },
          { "name": "M2a",   "type": { "type": "array", "items": "double" } },
          { "name": "M2ar",  "type": { "type": "array", "items": "double" } },
          { "name": "M2as",  "type": { "type": "array", "items": "double" } },
          { "name": "M2at",  "type": { "type": "array", "items": "double" } },
          { "name": "M2vz",  "type": { "type": "array", "items": "double" } },
          { "name": "M2vx",  "type": { "type": "array", "items": "double" } },
          { "name": "M2tmp", "type": { "type": "array", "items": "double" } },

          { "name": "pressure", "type": { "type": "array", "items": "double" } },
          { "name": "level",    "type": { "type": "array", "items": "double" } },
          { "name": "temp",     "type": { "type": "array", "items": "double" } },

          { "name": "ts", "type": "string" }
        ]
      }
    }
  ]
}
"""

# Your Avro-shaped payload
iot_data = [
    {
        "d": {
            "M1kwh": [76173.9140625],
            "M1kw": [0.0],
            "M1v": [400.12820434570312],
            "M1a": [0.0],
            "M1ar": [0.0],
            "M1as": [0.0],
            "M1at": [0.0],
            "M1vz": [0.93400001525878906],
            "M1vx": [0.49099999666213989],
            "M1tmp": [38.240001678466797],
            "M2kwh": [89896.4140625],
            "M2kw": [0.166585773229599],
            "M2v": [400.06747436523438],
            "M2a": [0.73965781927108765],
            "M2ar": [1.1516735553741455],
            "M2as": [1.0672998428344727],
            "M2at": [0.0],
            "M2vz": [0.15899999439716339],
            "M2vx": [0.26100000739097595],
            "M2tmp": [38.75],
            "pressure": [0.0],
            "level": [80.981155395507812],
            "temp": [47.479972839355469],
            "ts": "2025-11-17T07:57:48.941438"
        }
    }
]

# Schema Registry client and serializers
sr_client = SchemaRegistryClient(schema_registry_conf)

key_serializer = AvroSerializer(
    schema_registry_client=sr_client,
    schema_str=key_schema,
    to_dict=lambda key, ctx: key
)

value_serializer = AvroSerializer(
    schema_registry_client=sr_client,
    schema_str=value_schema,
    to_dict=lambda record, ctx: record
)

producer_conf = {
    **kafka_conf,
    "key.serializer": key_serializer,
    "value.serializer": value_serializer
}
producer = SerializingProducer(producer_conf)

topic_name = "iot"

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f'Message delivered to "{msg.topic()}" [partition {msg.partition()}] at offset {msg.offset()}')

for rec in iot_data:
    value_record = rec  # already matches value_schema
    key_record = {"M1kwh": rec["d"]["M1kwh"]}  # matches key_schema

    producer.produce(
        topic=topic_name,
        key=key_record,
        value=value_record,
        on_delivery=delivery_report
    )
    producer.poll(0)

producer.flush()