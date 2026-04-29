"""One-time Kafka topic setup. Idempotent."""
from __future__ import annotations

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from kafka_config import (
    DEFAULT_PARTITIONS,
    DEFAULT_REPLICATION,
    KAFKA_BOOTSTRAP,
    TOPIC_SCAN_REQUESTS,
    TOPIC_SCAN_RESULTS,
)


def ensure_topics() -> None:
    admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP, client_id="nse-scanner-admin")
    try:
        new_topics = [
            NewTopic(
                name=TOPIC_SCAN_REQUESTS,
                num_partitions=DEFAULT_PARTITIONS,
                replication_factor=DEFAULT_REPLICATION,
            ),
            NewTopic(
                name=TOPIC_SCAN_RESULTS,
                num_partitions=DEFAULT_PARTITIONS,
                replication_factor=DEFAULT_REPLICATION,
            ),
        ]
        try:
            admin.create_topics(new_topics=new_topics, validate_only=False)
            print(f"Created: {TOPIC_SCAN_REQUESTS}, {TOPIC_SCAN_RESULTS} "
                  f"({DEFAULT_PARTITIONS} partitions each)")
        except TopicAlreadyExistsError:
            print(f"Already exist: {TOPIC_SCAN_REQUESTS}, {TOPIC_SCAN_RESULTS}")
    finally:
        admin.close()


if __name__ == "__main__":
    ensure_topics()
