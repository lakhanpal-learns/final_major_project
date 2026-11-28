"""Standalone entry point for the independent AML Kafka consumer."""
import logging
import signal
import threading

from app.db.database import create_db_engine, create_schema, create_session_factory
from app.services.aml_event_consumer import (
    aml_consumer_settings, create_aml_kafka_consumer, run_aml_consumer,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("sentinel.aml_consumer")


def main() -> None:
    settings = aml_consumer_settings()
    engine = create_db_engine()
    create_schema(engine)
    session_factory = create_session_factory(engine)
    consumer = create_aml_kafka_consumer(topic=settings["topic"], group_id=settings["group_id"])
    stop_event = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop_event.set())
    signal.signal(signal.SIGTERM, lambda *_: stop_event.set())
    try:
        logger.info("AML consumer started: topic=%s group_id=%s", settings["topic"], settings["group_id"])
        run_aml_consumer(consumer, session_factory, stop_event=stop_event,
                         poll_timeout_ms=settings["poll_timeout_ms"],
                         on_result=lambda result: logger.info("AML event processing: %s", result))
    finally:
        consumer.close()
        engine.dispose()
        logger.info("AML consumer stopped")


if __name__ == "__main__":
    main()
