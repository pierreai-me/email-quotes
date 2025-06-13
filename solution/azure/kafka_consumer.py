import argparse
from kafka import KafkaConsumer
from solution.azure.models import BondQuote, KafkaSettings


def consume_quotes(env_file: str, count: int):
    """Consume bond quotes from Kafka"""
    settings = KafkaSettings(_env_file=env_file)  # type: ignore

    consumer = KafkaConsumer(
        settings.eventhub_name,
        **settings.get_kafka_config(),
        auto_offset_reset="earliest",  # Start from beginning if no offset
        enable_auto_commit=True,
        group_id="bond-quote-consumer-group",
        value_deserializer=lambda x: x.decode("utf-8"),
        consumer_timeout_ms=10_000,
    )

    print(
        f"Consuming {count} bond quotes from topic {settings.eventhub_name} broker {settings.kafka_broker} ..."
    )

    consumed_count = 0

    try:
        for message in consumer:
            try:
                quote = BondQuote.model_validate_json(message.value)
                consumed_count += 1
                print(f"Received quote {consumed_count}/{count}: {quote}")
                if consumed_count >= count:
                    break

            except Exception as e:
                print(f"Error parsing message {message.value}: {type(e)} {e}")

    except Exception as e:
        print(f"Error consuming messages: {type(e)} {e}")

    finally:
        consumer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Consume bond quotes from Kafka")
    parser.add_argument("--env-file", required=True, help="Path to .env file")
    parser.add_argument(
        "--count", type=int, default=10, help="Number of quotes to consume"
    )

    args = parser.parse_args()

    consume_quotes(args.env_file, args.count)
