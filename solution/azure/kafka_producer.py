# producer.py
import argparse
import random
from datetime import date, timedelta
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
    return BondQuote(
        ticker=random.choice(tickers),
        price=round(random.uniform(95.00, 105.00), 2),
        coupon=round(random.uniform(2.00, 8.00), 2),
        maturity=date.today() + timedelta(days=random.randint(365, 3650)),  # 1-10 years
    )


def produce_quotes(env_file: str, count: int):
    settings = KafkaSettings(_env_file=env_file)  # type: ignore

    producer = KafkaProducer(
        **settings.get_kafka_config(),
        value_serializer=lambda x: x.encode("utf-8"),
        retries=3,
        acks="all",
    )

    print(
        f"Producing {count} bond quotes to topic {settings.eventhub_name} broker {settings.kafka_broker} ..."
    )

    try:
        for i in range(count):
            quote = generate_random_bond_quote()

            future = producer.send(
                settings.eventhub_name, value=quote.model_dump_json()
            )

            # Wait for acknowledgment
            _ = future.get(timeout=10)

            print(f"Sent quote {i+1}/{count}: {quote}")

    except Exception as e:
        print(f"Error producing messages: {type(e)} {e}")
    finally:
        producer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Produce bond quotes to Kafka")
    parser.add_argument("--env-file", required=True, help="Path to .env file")
    parser.add_argument(
        "--count", type=int, default=10, help="Number of quotes to produce"
    )

    args = parser.parse_args()

    produce_quotes(args.env_file, args.count)
