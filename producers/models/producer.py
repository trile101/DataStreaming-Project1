"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer, CachedSchemaRegistryClient

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None, 
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas
        self.broker_properties = {
            "broker_url": "PLAINTEXT://localhost:9092",
            "schema_registry_url": "http://localhost:8081"
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            # Check the current topic
            client = AdminClient({"bootstrap.servers": self.broker_properties["broker_url"]})
            list_topics = client.list_topics()
            if self.topic_name not in list(list_topics.topics.keys()):
                self.create_topic()
                Producer.existing_topics.add(self.topic_name)

        schema_registry = CachedSchemaRegistryClient({"url": self.broker_properties["schema_registry_url"]})
        self.producer = AvroProducer(
            {"bootstrap.servers": self.broker_properties["broker_url"]},
            schema_registry=schema_registry
        )

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        client = AdminClient({"bootstrap.servers": self.broker_properties["broker_url"]})
        # Create a topic
        futures = client.create_topics(
            [
                NewTopic(
                    topic=self.topic_name,
                    num_partitions = self.num_partitions,
                    replication_factor = self.num_replicas,
                    config = {
                        "compression.type": "lz4",
                        "cleanup.policy": "delete",
                        "delete.retention.ms": 2,
                        "file.delete.delay.ms": 2
                    }
                )
            ]
        )
        for topic, future in futures.items():
            try:
                future.result()
                print("topic created")
            except Exception as e:
                print(f"failed to create topic {self.topic_name}: {e}")
                raise

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        self.producer.close()

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
