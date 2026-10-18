"""Kafka producer for raw webhook persistence."""

import logging
import os

logger = logging.getLogger("rekon.receiver.producer")

_producer = None


def get_kafka_bootstrap_servers() -> str:
    """Return Kafka bootstrap servers from environment or default."""
    return os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


def get_producer():
    """Lazy initialize Kafka producer."""
    global _producer
    if _producer is not None:
        return _producer

    try:
        from kafka import KafkaProducer

        bootstrap_servers = get_kafka_bootstrap_servers()
        _producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: v if isinstance(v, bytes) else str(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
            acks="all",
            retries=3,
            max_block_ms=3000,
        )
        logger.info("Kafka producer initialized connecting to %s", bootstrap_servers)
        return _producer
    except Exception as exc:
        logger.warning("Failed to initialize KafkaProducer: %s. Using mock fallback.", exc)
        return None


def publish_raw_event(topic: str, raw_payload: str, key: str | None = None) -> bool:
    """Publish raw payload string to specified Kafka topic without altering structure."""
    producer = get_producer()
    if producer is not None:
        try:
            future = producer.send(topic, value=raw_payload, key=key)
            producer.flush(timeout=2.0)
            record_metadata = future.get(timeout=2.0)
            logger.debug(
                "Published to %s partition=%d offset=%d",
                topic,
                record_metadata.partition,
                record_metadata.offset,
            )
            return True
        except Exception as exc:
            logger.error("Failed to publish event to topic %s: %s", topic, exc)
            return False
    else:
        logger.info("[MOCK-KAFKA] Emitted to topic '%s' (key=%s): %s", topic, key, raw_payload)
        return True
