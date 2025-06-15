import argparse
import random
import time
from datetime import date, datetime, timedelta
from kafka import KafkaProducer
from solution.azure.models import BondQuote, KafkaSettings


def generate_random_bond_quote() -> BondQuote:
    tickers = [
        "AAPL",
        "GOOGL",
        "MSFT",
        "TSLA",
        "AMZN",
        "META",
        "NFLX",
        "NVDA",
        "AMD",
        "IBM",
    ]

    senders = [
        "bloomberg@bloomberg.com",
        "quotes@reuters.com",
        "market@wsj.com",
        "data@yahoo.com",
        "feeds@marketwatch.com",
    ]

    recipients = [
        "trader1@hedge-fund.com",
        "analyst@investment-bank.com",
        "portfolio@pension-fund.com",
        "desk@prop-trading.com",
        "research@asset-mgmt.com",
        "bonds@insurance-co.com",
        "fixed-income@mutual-fund.com",
        "trading@credit-union.org",
    ]

    num_recipients = random.randint(1, 4)

    return BondQuote(
        sender=random.choice(senders),
        recipient=random.sample(recipients, num_recipients),
        quote_timestamp=datetime.now(),
        ticker=random.choice(tickers),
        price=round(random.uniform(95.00, 105.00), 2),
        coupon=round(random.uniform(2.00, 8.00), 2),
        maturity=date.today() + timedelta(days=random.randint(365, 3650)),  # 1-10 years
    )


def produce_quotes(env_file: str, count: int, batch: bool):
    settings = KafkaSettings(_env_file=env_file)  # type: ignore
    config = settings.get_kafka_config()

    if batch:
        # Add configuration parameters specific to batch producer
        config["linger_ms"] = 10  # wait up to `linger_ms` ms to fill batch
        config["max_in_flight_requests_per_connection"] = 5
        config["acks"] = 1
        config["retries"] = 3
        config["batch_size"] = 16_384
        config["buffer_memory"] = 67_108_864
        # config["compression_type"] = "gzip"
        config["request_timeout_ms"] = 30_000
        config["delivery_timeout_ms"] = 120_000

    producer = KafkaProducer(
        **config,
        value_serializer=lambda x: x.encode("utf-8"),
    )

    print(
        f"Producing {count} bond quotes to topic {settings.eventhub_name} broker {settings.kafka_broker} batch {batch} ..."
    )

    stats = {
        "start_time": time.time(),
        "sent": 0,
        "failed": 0,
        "pending_futures": [],
    }

    def delivery_callback(record_metadata):
        stats["sent"] += 1

    def error_callback(exception):
        stats["failed"] += 1
        print(f"Failed to send message: {type(exception)} {exception}")

    ret = []

    try:
        for i in range(count):
            quote = generate_random_bond_quote()
            ret.append(quote)

            future = producer.send(
                topic=settings.eventhub_name, value=quote.model_dump_json()
            )

            if not batch:
                # Wait for acknowledgment
                _ = future.get(timeout=10)
                print(f"Sent quote {i+1}/{count}: {quote}")
            else:
                future.add_callback(delivery_callback)
                future.add_errback(error_callback)
                stats["pending_futures"].append(future)

        print("Flushing remaining messages...")
        producer.flush()

    except Exception as e:
        print(f"Error producing messages: {type(e)} {e}")
    finally:
        producer.close()

    elapsed = time.time() - stats["start_time"]
    rate = count / elapsed
    print(f"Sent {count} quotes in {elapsed:.1f} sec ({rate:.1f} msg/sec)")

    return ret


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Produce bond quotes to Kafka")
    parser.add_argument("--env-file", required=True, help="Path to .env file")
    parser.add_argument(
        "--count", type=int, default=10, help="Number of quotes to produce"
    )
    parser.add_argument(
        "--batch",
        action="store_true",
        default=False,
        help="Send quotes in batches (default: single quote at a time)",
    )

    args = parser.parse_args()

    produce_quotes(args.env_file, args.count, args.batch)
