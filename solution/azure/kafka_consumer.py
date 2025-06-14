import argparse
import uuid
import psycopg2
from azure.cosmos import CosmosClient
from datetime import datetime
from kafka import KafkaConsumer
from solution.azure.models import (
    BondQuote,
    CosmosSettings,
    KafkaSettings,
    PostgresSettings,
    SqlSettings,
)


class SqlInserter:

    def __init__(self, settings: SqlSettings):
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


class CosmosInserter:

    def __init__(self, settings: CosmosSettings):
        self.settings = settings
        self.client = CosmosClient(settings.cosmos_endpoint, settings.cosmos_key)
        self.database = self.client.get_database_client(settings.cosmos_database)
        self.container = self.database.get_container_client(settings.cosmos_container)

    def insert_quote(self, quote: BondQuote) -> None:
        try:
            # Create document in denormalized format
            document = {
                "id": f"quote_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:8]}",
                "sender": quote.sender,
                "recipients": quote.recipient,  # Store as array
                "quote_timestamp": quote.quote_timestamp.isoformat(),
                "ticker": quote.ticker,
                "price": quote.price,
                "coupon": quote.coupon,
                "maturity": quote.maturity.isoformat(),
                "created_at": datetime.now().isoformat(),
            }

            self.container.create_item(document)

        except Exception as e:
            print(f"Error inserting quote into Cosmos DB: {type(e)} {e}")
            raise

    def close(self):
        # Cosmos client doesn't need explicit closing
        pass


def consume_quotes(
    env_file: str, count: int, insert_sql: bool = False, insert_cosmos: bool = False
):
    settings = KafkaSettings(_env_file=env_file)  # type: ignore

    sql_inserter = None
    cosmos_inserter = None

    if insert_sql:
        print(f"Will write to SQL DB")
        db_settings = PostgresSettings(_env_file=env_file)  # type: ignore
        sql_inserter = SqlInserter(db_settings)

    if insert_cosmos:
        print(f"Will write to NoSQL DB")
        cosmos_settings = CosmosSettings(_env_file=env_file)  # type: ignore
        cosmos_inserter = CosmosInserter(cosmos_settings)

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

                if cosmos_inserter:
                    cosmos_inserter.insert_quote(quote)

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
        if cosmos_inserter:
            cosmos_inserter.close()


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
    parser.add_argument(
        "--insert-no-sql",
        action="store_true",
        default=False,
        help="Insert quotes into Cosmos DB (default: False)",
    )

    args = parser.parse_args()

    consume_quotes(args.env_file, args.count, args.insert_sql, args.insert_no_sql)
