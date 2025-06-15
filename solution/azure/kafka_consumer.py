import argparse
import concurrent.futures
import threading
import time
import psycopg2
from azure.cosmos import CosmosClient
from datetime import datetime
from kafka import KafkaConsumer, TopicPartition
from psycopg2.extras import execute_values
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
        if not self.quote_batch:
            return

        cursor = self.connection.cursor()
        try:
            quote_insert_query = """
                INSERT INTO quotes (sender, quote_timestamp, ticker, price, coupon, maturity)
                VALUES %s
                RETURNING id
            """

            cursor.execute("BEGIN")

            execute_values(
                cursor,
                quote_insert_query,
                self.quote_batch,
                template=None,
                page_size=len(self.quote_batch),
            )

            quote_ids = [row[0] for row in cursor.fetchall()]

            # Prepare recipient data with actual quote IDs
            if self.recipient_batch and quote_ids:
                recipient_inserts = []
                for batch_position, recipient in self.recipient_batch:
                    if batch_position < len(quote_ids):
                        quote_id = quote_ids[batch_position]
                        recipient_inserts.append((quote_id, recipient))

                if recipient_inserts:
                    # Use execute_values for bulk recipient insertion
                    recipient_insert_query = """
                        INSERT INTO quote_recipients (quote_id, recipient)
                        VALUES %s
                    """
                    execute_values(
                        cursor,
                        recipient_insert_query,
                        recipient_inserts,
                        template=None,
                        page_size=len(recipient_inserts),
                    )

            cursor.execute("COMMIT")
            print(f"Inserted {len(self.quote_batch)} quotes to SQL DB")

        except Exception as e:
            print(f"Error inserting batch into database: {type(e)} {e}")
            cursor.execute("ROLLBACK")
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
        self._counter = 0
        self._counter_lock = threading.Lock()

    def _generate_unique_id(self) -> str:
        """Generate a unique ID for the document."""
        with self._counter_lock:
            self._counter += 1
            return (
                f"quote_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{self._counter:06d}"
            )

    def add_quote(self, quote: BondQuote) -> None:
        """Add a quote to the batch for later insertion."""
        document = {
            "id": self._generate_unique_id(),
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

    def _insert_document(self, document: dict) -> tuple:
        """Insert a single document and return result."""
        try:
            self.container.create_item(document)
            return True, document["id"], None
        except Exception as e:
            return False, document["id"], str(e)

    def flush(self) -> None:
        """Insert all batched documents using parallel execution."""
        if not self.document_batch:
            return

        batch_size = len(self.document_batch)

        try:

            successful_inserts = 0
            failed_inserts = 0

            # Use ThreadPoolExecutor for parallel inserts
            # Cosmos DB can handle concurrent requests well
            max_workers = min(batch_size, 20)  # Cap concurrent requests

            with concurrent.futures.ThreadPoolExecutor(
                max_workers=max_workers
            ) as executor:
                # Submit all insert tasks
                future_to_doc = {
                    executor.submit(self._insert_document, doc): doc
                    for doc in self.document_batch
                }

                # Collect results
                for future in concurrent.futures.as_completed(future_to_doc):
                    success, doc_id, error = future.result()
                    if success:
                        successful_inserts += 1
                    else:
                        failed_inserts += 1
                        print(f"Failed to insert document {doc_id}: {error}")

            print(
                f"Inserted {successful_inserts} quotes to Cosmos DB"
                + (f" ({failed_inserts} failed)" if failed_inserts > 0 else "")
            )

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
    start_timestamp: float | None = None,
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
        **kafka_config,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        auto_commit_interval_ms=5000,  # Commit offsets
        group_id="bond-quote-consumer-group-2",
        value_deserializer=lambda x: x.decode("utf-8"),
        consumer_timeout_ms=30_000,
    )

    # Seek to messages after start_timestamp if provided
    if start_timestamp:
        print(f"Seeking to messages after timestamp {start_timestamp}")

        # Get all partitions for the topic and assign them
        topic_partitions = consumer.partitions_for_topic(settings.eventhub_name)
        if topic_partitions:
            partitions = [TopicPartition(settings.eventhub_name, p) for p in topic_partitions]
            consumer.assign(partitions)

            # Convert timestamp to milliseconds and create timestamp dict
            target_timestamp_ms = int(start_timestamp * 1000)
            timestamp_dict = {tp: target_timestamp_ms for tp in partitions}

            # Get offsets for the timestamp
            offsets = consumer.offsets_for_times(timestamp_dict)

            # Seek to those offsets
            for tp, offset_metadata in offsets.items():
                if offset_metadata:
                    consumer.seek(tp, offset_metadata.offset)
                    print(f"Seeked partition {tp.partition} to offset {offset_metadata.offset}")
    else:
        # Subscribe to topic normally if no timestamp filtering
        consumer.subscribe([settings.eventhub_name])

    print(
        f"Consuming {count} bond quotes from topic {settings.eventhub_name} "
        f"broker {settings.kafka_broker} with batch size {batch_size}..."
    )

    consumed_count = 0
    start_time = time.time()
    last_commit_time = start_time
    ret = []

    try:
        for message in consumer:
            try:
                quote = BondQuote.model_validate_json(message.value)

                # Skip messages older than start_timestamp if provided
                if start_timestamp and quote.quote_timestamp.timestamp() < start_timestamp:
                    continue

                ret.append(quote)
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
    rate = consumed_count / elapsed if elapsed > 0 else 0
    print(f"Received {consumed_count} quotes in {elapsed:.1f} sec ({rate:.1f} msg/sec)")

    return ret


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
    parser.add_argument(
        "--start-timestamp",
        type=float,
        help="Unix timestamp - only consume messages after this time",
    )

    args = parser.parse_args()

    consume_quotes(
        args.env_file,
        args.count,
        args.insert_sql,
        args.insert_no_sql,
        args.batch_size,
        args.commit_interval,
        args.start_timestamp,
    )