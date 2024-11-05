from airflow import DAG
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow.providers.apache.kafka.hooks.produce import KafkaProducerHook
from datetime import datetime

# Kafka connection ID defined in Airflow's connection settings
KAFKA_CONN_ID = 'kafka_default'

# The Kafka topic where the message will be sent
KAFKA_TOPIC = 'example_topic'

# The message payload to be sent to Kafka
message_payload = 'Hello, Kafka! This is a message from Airflow.'

# Default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 11, 5),
    'retries': 1,
}

# Define the DAG
with DAG(
    'produce_consume_treats',
    default_args=default_args,
    schedule_interval=None,  # This is triggered manually
    catchup=False,
    tags=['example', 'kafka'],
) as dag:

    # Task to produce a message to the Kafka topic
    produce_message = ProduceToTopicOperator(
        task_id='send_message_to_kafka',
        kafka_conn_id=KAFKA_CONN_ID,
        topic=KAFKA_TOPIC,
        message=message_payload,
        value_serializer='json',  # Serializer (e.g., json or simple string)
    )

    # Set task dependencies
    produce_message
