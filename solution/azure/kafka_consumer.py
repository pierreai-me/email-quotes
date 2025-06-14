import argparse
import time
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
    def __init__(self, settings: SqlSettings, batch_size: int):
        self.settings = settings
        self.batch_size = batch_size
        conn_string = settings.get_connection_string()
        self.connection = psycopg2.connect(conn_string)
        self.connection.autocommit = False  # Ensure transactions are used
        self.quote_batch = []
        self.recipient_batch = []

    def add_quote(self, quote: BondQuote) -> None:
        """Add a quote to the batch for later insertion."""
        self.quote_batch.append(
            (
                quote.sender,
                quote.quote_timestamp,
                quote.ticker,
                quote.price,
                quote.coupon,
                quote.maturity,
            )
        )

        # Store recipients with their position in the batch for later linking
        batch_position = len(self.quote_batch) - 1
        for recipient in quote.recipient:
            self.recipient_batch.append((batch_position, recipient))

        # Auto-flush if batch is full
        if len(self.quote_batch) >= self.batch_size:
            self.flush()

    def flush(self) -> None:
        """Insert all batched quotes in a single transaction."""
        if not self.quote_batch:
            return

        cursor = self.connection.cursor()
        try:
            # Insert quotes in batch and get their IDs
            cursor.executemany(
                """
                INSERT INTO quotes (sender, quote_timestamp, ticker, price, coupon, maturity)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                self.quote_batch,
            )

            # Get the IDs of the inserted quotes
            cursor.execute(
                "SELECT id FROM quotes ORDER BY id DESC LIMIT %s",
                (len(self.quote_batch),),
            )
            quote_ids = [row[0] for row in cursor.fetchall()]
            quote_ids.reverse()  # Reverse to match insertion order

            # Insert recipients in batch
            recipient_inserts = []
            for batch_position, recipient in self.recipient_batch:
                quote_id = quote_ids[batch_position]
                recipient_inserts.append((quote_id, recipient))

            if recipient_inserts:
                cursor.executemany(
                    """
                    INSERT INTO quote_recipients (quote_id, recipient)
                    VALUES (%s, %s)
                    """,
                    recipient_inserts,
                )

            self.connection.commit()
            print(f"Inserted {len(self.quote_batch)} quotes to SQL DB")

        except Exception as e:
            print(f"Error inserting batch into database: {type(e)} {e}")
            self.connection.rollback()
            raise

        finally:
            cursor.close()
            self.quote_batch.clear()
            self.recipient_batch.clear()

    def close(self):
        self.flush()
        self.connection.close()


class CosmosInserter:
    def __init__(self, settings: CosmosSettings, batch_size: int):
        self.settings = settings
        self.batch_size = batch_size
        self.client = CosmosClient(settings.cosmos_endpoint, settings.cosmos_key)
        self.database = self.client.get_database_client(settings.cosmos_database)
        self.container = self.database.get_container_client(settings.cosmos_container)
        self.document_batch = []

    def add_quote(self, quote: BondQuote) -> None:
        """Add a quote to the batch for later insertion."""
        document = {
            "id": f"quote_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:8]}",
            "sender": quote.sender,
            "recipients": quote.recipient,
            "quote_timestamp": quote.quote_timestamp.isoformat(),
            "ticker": quote.ticker,
            "price": quote.price,
            "coupon": quote.coupon,
            "maturity": quote.maturity.isoformat(),
            "created_at": datetime.now().isoformat(),
        }
        self.document_batch.append(document)

        # Auto-flush if batch is full
        if len(self.document_batch) >= self.batch_size:
            self.flush()

    def flush(self) -> None:
        """Insert all batched documents."""
        if not self.document_batch:
            return

        try:
            # Cosmos DB doesn't have native batch insert, but we can parallelize
            # For now, we'll use sequential inserts but within a smaller batch
            for document in self.document_batch:
                self.container.create_item(document)

            print(f"Inserted {len(self.document_batch)} quotes to Cosmos DB")

        except Exception as e:
            print(f"Error inserting batch into Cosmos DB: {type(e)} {e}")
            raise

        finally:
            self.document_batch.clear()

    def close(self):
        self.flush()


def consume_quotes(
    env_file: str,
    count: int,
    insert_sql: bool,
    insert_cosmos: bool,
    batch_size: int,
    commit_interval: int,
):
    settings = KafkaSettings(_env_file=env_file)  # type: ignore

    sql_inserter = None
    cosmos_inserter = None

    if insert_sql:
        print(f"Will write to SQL DB with batch size {batch_size}")
        db_settings = PostgresSettings(_env_file=env_file)  # type: ignore
        sql_inserter = SqlInserter(db_settings, batch_size)

    if insert_cosmos:
        print(f"Will write to NoSQL DB with batch size {batch_size}")
        cosmos_settings = CosmosSettings(_env_file=env_file)  # type: ignore
        cosmos_inserter = CosmosInserter(cosmos_settings, batch_size)

    # Optimize Kafka consumer settings
    kafka_config = settings.get_kafka_config()
    kafka_config.update(
        {
            "fetch_min_bytes": 1024,  # Wait for at least 1KB of data
            "fetch_max_wait_ms": 500,  # Or wait max 500ms
            "max_poll_records": batch_size,  # Fetch more records per poll
            "receive_buffer_bytes": 65_536,  # Increase receive buffer
            "send_buffer_bytes": 65_536,  # Increase send buffer
        }
    )

    consumer = KafkaConsumer(
        settings.eventhub_name,
        **kafka_config,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        auto_commit_interval_ms=5000,  # Commit offsets
        group_id="bond-quote-consumer-group-2",
        value_deserializer=lambda x: x.decode("utf-8"),
        consumer_timeout_ms=30_000,
    )

    print(
        f"Consuming {count} bond quotes from topic {settings.eventhub_name} "
        f"broker {settings.kafka_broker} with batch size {batch_size}..."
    )

    consumed_count = 0
    start_time = time.time()
    last_commit_time = start_time

    try:
        for message in consumer:
            try:
                quote = BondQuote.model_validate_json(message.value)
                consumed_count += 1
                print(f"Received quote {consumed_count}/{count}: {quote}")

                if sql_inserter:
                    sql_inserter.add_quote(quote)

                if cosmos_inserter:
                    cosmos_inserter.add_quote(quote)

                # Periodic flush for better performance monitoring
                current_time = time.time()
                if current_time - last_commit_time >= commit_interval:
                    if sql_inserter:
                        sql_inserter.flush()
                    if cosmos_inserter:
                        cosmos_inserter.flush()
                    last_commit_time = current_time

                if consumed_count >= count:
                    break

            except Exception as e:
                print(f"Error processing message {message.value}: {type(e)} {e}")
                continue

    except Exception as e:
        print(f"Error processing messages: {type(e)} {e}")

    finally:
        # Final flush of remaining items
        print("Flushing remaining items...")
        if sql_inserter:
            sql_inserter.close()
        if cosmos_inserter:
            cosmos_inserter.close()
        consumer.close()

    elapsed = time.time() - start_time
    rate = consumed_count / elapsed
    print(f"Received {consumed_count} quotes in {elapsed:.1f} sec ({rate:.1f} msg/sec)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Consume bond quotes from Kafka with batched DB operations"
    )
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
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1,
        help="Number of quotes to batch before database insertion (default: 1)",
    )
    parser.add_argument(
        "--commit-interval",
        type=int,
        default=10,
        help="Seconds between forced batch flushes (default: 10)",
    )

    args = parser.parse_args()

    consume_quotes(
        args.env_file,
        args.count,
        args.insert_sql,
        args.insert_no_sql,
        args.batch_size,
        args.commit_interval,
    )
