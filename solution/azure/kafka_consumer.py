import argparse
from datetime import datetime
import psycopg2
from kafka import KafkaConsumer
from solution.azure.models import BondQuote, DatabaseSettings, KafkaSettings


class SqlInserter:

    def __init__(self, settings: DatabaseSettings):
        self.settings = settings
        conn_string = settings.get_connection_string()
        self.connection = psycopg2.connect(conn_string)

    def insert_quote(self, quote: BondQuote) -> None:
        cursor = self.connection.cursor()
        try:
            cursor.execute(
                """
                INSERT INTO quotes (sender, quote_timestamp, ticker, price, coupon, maturity)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    quote.sender,
                    datetime.now(),
                    quote.ticker,
                    quote.price,
                    quote.coupon,
                    quote.maturity,
                ),
            )

            quote_id = cursor.fetchone()[0]  # type: ignore

            for recipient in quote.recipient:
                cursor.execute(
                    """
                    INSERT INTO quote_recipients (quote_id, recipient)
                    VALUES (%s, %s)
                    """,
                    (quote_id, recipient),
                )

            self.connection.commit()

        except Exception as e:
            print(f"Error inserting quote into database: {type(e)} {e}")
            self.connection.rollback()
            raise

        finally:
            cursor.close()

    def close(self):
        self.connection.close()


def consume_quotes(env_file: str, count: int, insert_sql: bool = False):
    """Consume bond quotes from Kafka"""
    settings = KafkaSettings(_env_file=env_file)  # type: ignore

    # Initialize database inserter if needed
    sql_inserter = None
    if insert_sql:
        db_settings = DatabaseSettings(_env_file=env_file)  # type: ignore
        sql_inserter = SqlInserter(db_settings)

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

                if sql_inserter:
                    sql_inserter.insert_quote(quote)

                if consumed_count >= count:
                    break

            except Exception as e:
                print(f"Error parsing message {message.value}: {type(e)} {e}")

    except Exception as e:
        print(f"Error consuming messages: {type(e)} {e}")

    finally:
        consumer.close()
        if sql_inserter:
            sql_inserter.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Consume bond quotes from Kafka")
    parser.add_argument("--env-file", required=True, help="Path to .env file")
    parser.add_argument(
        "--count", type=int, default=10, help="Number of quotes to consume"
    )
    parser.add_argument(
        "--insert-sql",
        action="store_true",
        default=False,
        help="Insert quotes into PostgreSQL database (default: False)",
    )

    args = parser.parse_args()

    consume_quotes(args.env_file, args.count, args.insert_sql)
