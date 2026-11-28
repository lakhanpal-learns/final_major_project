"""Standalone process entry point for the fraud Kafka consumer."""

import logging
import signal
import threading

from app.db.database import create_db_engine, create_schema, create_session_factory
from app.services.fraud_event_consumer import (
    create_fraud_kafka_consumer,
    fraud_consumer_settings,
    run_fraud_consumer,
)
from app.services.fraud_inference_service import FraudInferenceService

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("sentinel.fraud_consumer")


def main() -> None:
    settings = fraud_consumer_settings()
    engine = create_db_engine()
    create_schema(engine)
    session_factory = create_session_factory(engine)
    inference_service = FraudInferenceService.from_environment()
    consumer = create_fraud_kafka_consumer(
        topic=settings["topic"],
        group_id=settings["group_id"],
    )
    stop_event = threading.Event()

    signal.signal(signal.SIGINT, lambda *_: stop_event.set())
    signal.signal(signal.SIGTERM, lambda *_: stop_event.set())

    try:
        logger.info(
            "Fraud consumer started: topic=%s group_id=%s",
            settings["topic"],
            settings["group_id"],
        )
        run_fraud_consumer(
            consumer,
            session_factory,
            stop_event=stop_event,
            poll_timeout_ms=settings["poll_timeout_ms"],
            inference_service=inference_service,
            on_result=lambda result: logger.info("Fraud event processing: %s", result),
        )
    finally:
        consumer.close()
        engine.dispose()
        logger.info("Fraud consumer stopped")


if __name__ == "__main__":
    main()
