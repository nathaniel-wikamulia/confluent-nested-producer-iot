#  Kafka Producer for Nested IOT Data
In this workshop, you will learn how to create a Python Kafka producer, process nested IoT data structures, enforce schema compliance using Schema Registry, send data to Confluent Cloud, and process the data using Flink.

## Prerequisites
1. Download the code from this repository.
2. Prepare a Confluent Cloud environment and cluster that is accessible either via the public internet or a private network.
3. Prepare a Flink compute pool.
3. Generate a Confluent Cloud cluster API key and a Schema Registry API key.
4. Install the Confluent Kafka Python client along with optional JSON Schema and performance dependencies:
```
sudo apt install python3-pip
pip install confluent-kafka
pip install confluent-kafka-schema-registry
```

## Step 1 - Create an Apache Kafka topic
After creating a Confluent Cloud cluster, we'll create two new Apache Kafka topics called "iot" and "iot_unnested".

Topics are the core abstraction for storing and processing data. Unlike traditional databases where data is stored in tables and updated with each new event, Kafka uses logs to record every event in sequence.

To create a topic, follow the steps below:
1. Click the previously provisioned Confluent Cloud cluster.
2. In the left-hand menu, select "Topics".
3. Click the "Add topic" button on the right side of the page.
4. Enter a name for your topic. For this workshop, we’ll use "iot".
5. Click on the "Create with defaults" button.
6. Repeat the steps above and create another topic called "iot_unnested".

## Step 2 - Run the Apache Kafka producer
Once you have the API key and secret for the Cluster and Schema Registry, you can start creating the Kafka producer.

A Kafka producer is an application or client that sends (or “produces”) messages to Kafka topics. To create an Apache Kafka producer, you can follow the steps below:
1. Open the "kafka_producer_nested.py" file.
2. Replace the "`<CLOUD CLUSTER BOOTSTRAP SERVER>`" string in the "kafka_producer_nested.py" file using the "Bootstrap server" information from the cluster API key ".txt" file you downloaded after you generate the cluster API key.
3. Replace the "`<CLOUD CLUSTER API KEY>`" string in the "kafka_producer_nested.py" file using the "API key" information from the cluster API key ".txt" file you downloaded after you generate the cluster API key.
4. Replace the "`<CLOUD CLUSTER API SECRET>`" string in the "kafka_producer_nested.py" file using the "API secret" information from the cluster API key ".txt" file you downloaded after you generate the cluster API key.
5. Replace the "`<SCHEMA REGISTRY PUBLIC ENDPOINT>`" string in the "kafka_producer_nested.py" file using the "Endpoint" information from the schema registry API key ".txt" file you downloaded after you generate the schema registry API key.
6. Replace the "`<SCHEMA REGISTRY API KEY>`" string in the "kafka_producer_nested.py" file using the "API key" information from the schema registry API key ".txt" file you downloaded after you generate the schema registry API key.
7. Replace the "`<SCHEMA REGISTRY API SECRET>`" string in the "kafka_producer_nested.py" file using the "API secret" information from the schema registry API key ".txt" file you downloaded after you generate the schema registry API key.
8. Open a terminal in the directory where your "kafka_producer_nested.py" file is located.
9. Run the following command to execute the "kafka_producer_nested.py" script:
```
python3 kafka_producer_nested.py  
```
10. If you see a message similar to the one below, you have successfully sent data to the "iot" Kafka topic:
```
Message delivered to "iot" [partition 0] at offset 35
```
11. Repeat the steps above using the "kafka_producer_unnested.py" file, which generates data for the "iot_unnested" topic.

## Step 3 - Run a Transformation Query using Flink
Now we will run a Flink query on top of a Flink compute pool. A Flink compute pool in Confluent Cloud for Apache Flink represents a set of compute resources bound to a region that is used to run your SQL statements. To write Flink queries on a Flink compute pool, you can follow the steps below:
1. Click the "Open SQL workspace" button on the right side of the page.
2. Open the "Catalog" dropdown in the top-right corner and select your previously provisioned environment.
3. Open the "Database" dropdown in the top right corner and choose your previously provisioned Kafka cluster.
4. Paste the following Flink query into an empty cell:
```
CREATE TABLE alert_topic_unnested
DISTRIBUTED BY (M1kwh) INTO 6 BUCKETS
WITH (
  'key.format' = 'avro-registry',
  'value.format' = 'avro-registry'
)
AS
SELECT 
  M1kwh[1] AS M1kwh,
  CONCAT(
    'Alert! the temperature of device_id ',
    CAST(M1kwh[1] AS STRING),
    ' is ',
    CAST(temp[1] AS STRING),
    ' degrees Celsius'
  ) AS alert_msg
FROM `iot_unnested`
WHERE temp[1] > 100 ;
```
5. Click the "Run" button on the right side of the cell.
6. After you execute the Flink query, it will begin inserting the filtered data into a new Kafka topic called "alert_topic_unnested". If you see messages appearing in "alert_topic_unnested", it means your query ran successfully.
