"""Command-line entry point for the standalone outbox publisher."""

import logging
import signal
import threading

from app.db.database import create_db_engine, create_session_factory
from app.services.outbox_publisher import (
    create_kafka_producer,
    publisher_settings,
    run_publisher_loop,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("sentinel.outbox_publisher")


def main() -> None:
    settings = publisher_settings()
    engine = create_db_engine()
    session_factory = create_session_factory(engine)
    producer = create_kafka_producer()
    stop_event = threading.Event()

    signal.signal(signal.SIGINT, lambda *_: stop_event.set())
    signal.signal(signal.SIGTERM, lambda *_: stop_event.set())

    try:
        run_publisher_loop(
            session_factory,
            producer,
            stop_event=stop_event,
            on_batch=lambda stats: logger.info("Outbox batch: %s", stats),
            **settings,
        )
    finally:
        producer.flush(timeout=10)
        producer.close(timeout=10)
        engine.dispose()


if __name__ == "__main__":
    main()
