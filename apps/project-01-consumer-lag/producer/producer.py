"""Trade event producer for the Meridian positions pipeline.

Emits synthetic TRADE_EXECUTED events to the `trades-events` Kafka topic at a
steady rate, keyed by ticker symbol so all trades for a symbol land on one
partition — the property the consumer-lag scenario depends on.
"""

import json
import os
import random
import signal
import time
import uuid
from datetime import datetime, timezone
from types import FrameType
from typing import Any

from confluent_kafka import KafkaError, Message, Producer

# Fixed ticker universe. Baseline load spreads across these; the Module 1
# volatility spike later concentrates volume on one (VOLT).
SYMBOLS = [
    "VOLT", "AAPL", "MSFT", "AMZN", "NVDA", "GOOGL", "META", "TSLA", "JPM", "V",
    "JNJ", "WMT", "MA", "PG", "HD", "CVX", "ABBV", "KO", "PEP", "COST",
    "MRK", "ADBE", "CRM", "NFLX", "AMD", "INTC", "CSCO", "TMO", "ACN", "MCD",
    "ABT", "DHR", "NKE", "TXN", "LIN", "WFC", "DIS", "BMY", "PM", "UPS",
    "MS", "RTX", "HON", "QCOM", "GS", "CAT", "BA", "GE", "SBUX", "GME",
]

# Synthetic user population.
NUM_USERS = 10000


class GracefulShutdown:
    """Trips a stop flag on SIGTERM/SIGINT so the produce loop can exit cleanly."""

    def __init__(self) -> None:
        """Installs SIGTERM/SIGINT handlers and initializes the stop flag."""
        self.stop = False
        signal.signal(signal.SIGTERM, self._handle)
        signal.signal(signal.SIGINT, self._handle)

    def _handle(self, signum: int, frame: FrameType | None) -> None:
        """Requests shutdown when a termination signal arrives."""
        self.stop = True


def load_config() -> dict[str, Any]:
    """Reads producer settings from the environment, with local defaults."""
    return {
        "bootstrap_servers": os.environ.get(
            "KAFKA_BOOTSTRAP_SERVERS", "meridian-kafka-kafka-bootstrap:9092"
        ),
        "topic": os.environ.get("KAFKA_TOPIC", "trades-events"),
        "rate_per_sec": float(os.environ.get("TRADE_RATE_PER_SEC", "25")),
    }


def build_producer(bootstrap_servers: str) -> Producer:
    """Creates a durable, idempotent Kafka producer.

    acks=all and idempotence uphold the topic's min.insync.replicas=2
    durability contract and prevent duplicate records on retry.
    """
    return Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "acks": "all",
            "enable.idempotence": True,
            "client.id": "trade-producer",
        }
    )


def make_trade() -> tuple[str, dict]:
    """Builds one synthetic TRADE_EXECUTED event and its partition key."""
    symbol = random.choice(SYMBOLS)
    trade = {
        "trade_id": str(uuid.uuid4()),
        "user_id": f"user-{random.randint(1, NUM_USERS)}",
        "symbol": symbol,
        "side": random.choice(["BUY", "SELL"]),
        "quantity": round(random.uniform(1, 100), 2),
        "price": round(random.uniform(10, 500), 2),
        "filled_at": datetime.now(timezone.utc).isoformat(),
    }
    return symbol, trade


def on_delivery(err: KafkaError | None, msg: Message) -> None:
    """Delivery callback — surface failures so dropped trades are visible."""
    if err is not None:
        print(f"delivery failed for key={msg.key()}: {err}", flush=True)


def main() -> None:
    """Producer entrypoint."""
    cfg = load_config()
    producer = build_producer(cfg["bootstrap_servers"])
    interval = 1.0 / cfg["rate_per_sec"]
    shutdown = GracefulShutdown()
    print(f"producing to {cfg['topic']} at {cfg['rate_per_sec']}/s", flush=True)

    try:
        while not shutdown.stop:
            symbol, trade = make_trade()
            producer.produce(
                cfg["topic"],
                key=symbol.encode("utf-8"),
                value=json.dumps(trade).encode("utf-8"),
                on_delivery=on_delivery,
            )
            producer.poll(0)
            time.sleep(interval)
    finally:
        remaining = producer.flush(timeout=10)
        print(f"shutting down, {remaining} messages undelivered", flush=True)


if __name__ == "__main__":
    main()
