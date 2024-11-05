"""
### DAG which produces to and consumes from a Kafka cluster

This DAG will produce messages consisting of several elements to a Kafka cluster and consume
them.
"""

from airflow.decorators import dag, task
from pendulum import datetime
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
import json
import random

YOUR_NAME = "Vinod"
YOUR_PET_NAME = "kutta"
NUMBER_OF_TREATS = 5
KAFKA_TOPIC = "kutta_topic"


def prod_function(**context):
    """Produces `num_treats` messages containing the pet's name, a randomly picked
    pet mood post treat and whether or not it was the last treat in a series."""

    num_treats = 5
    pet_name = "kutta"
    for i in range(num_treats):
        final_treat = False
        pet_mood_post_treat = random.choices(
            ["content", "happy", "zoomy", "bouncy"], weights=[2, 2, 1, 1], k=1
        )[0]
        if i + 1 == num_treats:
            final_treat = True
        yield (
            json.dumps(i),
            json.dumps(
                {
                    "pet_name": pet_name,
                    "pet_mood_post_treat": pet_mood_post_treat,
                    "final_treat": final_treat,
                }
            ),
        )


def consume_function(message, name, **context):
    "Takes in consumed messages and prints its contents to the logs."
    message = "hello world"
    print(
        f"Message {message}!"
    )


@dag(
    start_date=datetime(2023, 4, 1),
    schedule=None,
    catchup=False,
    render_template_as_native_obj=True,
)
def produce_consume_treats():
    @task
    def get_your_pet_name(pet_name=None):
        return pet_name

    @task
    def get_number_of_treats(num_treats=None):
        return num_treats

    @task
    def get_pet_owner_name(your_name=None):
        return your_name

    produce_treats = ProduceToTopicOperator(
        task_id="produce_treats",
        kafka_config_id="kafka_default",
        topic=KAFKA_TOPIC,
        producer_function=prod_function,
        producer_function_args=(b"test", b"test"),
        poll_timeout=10,
    )

    consume_treats = ConsumeFromTopicOperator(
        task_id="consume_treats",
        kafka_config_id="kafka_default",
        topics=[KAFKA_TOPIC],
        apply_function=consume_function,
        apply_function_kwargs={
            "name": "{{ ti.xcom_pull(task_ids='get_pet_owner_name')}}"
        },
        poll_timeout=20,
        max_messages=20,
        max_batch_size=20,
    )

    [
        get_your_pet_name(YOUR_PET_NAME),
        get_number_of_treats(NUMBER_OF_TREATS),
    ] >> produce_treats
    get_pet_owner_name(YOUR_NAME) >> consume_treats

    produce_treats >> consume_treats


produce_consume_treats()
