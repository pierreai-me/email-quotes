#!/usr/bin/env python3
"""
REST endpoints for bond quotes with PostgreSQL-based offset tracking.
- GET /quote - returns a single quote
- GET /quotes?batch_size=N - returns multiple quotes (default 10, max 10,000)

Features:
1. Fast manual partition assignment (no consumer group overhead)
2. PostgreSQL-based offset tracking for progressive consumption
3. Each request gets the next batch of quotes
4. Stateless server design with persistent offset state
"""

import sys
import time
from contextlib import asynccontextmanager
from typing import List, Optional, Dict
from fastapi import FastAPI, Query, HTTPException, status
from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaTimeoutError, KafkaError
from .models import BondQuote, KafkaSettings, PostgresSettings
import psycopg2
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Consumer ID for this server instance
CONSUMER_ID = "bond-quote-server-progressive"


class PostgresOffsetManager:
    """Manage Kafka offsets using PostgreSQL for persistence."""

    def __init__(self, env_file: Optional[str] = None):
        if env_file:
            self.pg_settings = PostgresSettings(_env_file=env_file)  # type: ignore
        else:
            self.pg_settings = PostgresSettings()  # type: ignore

    def get_stored_offsets(self, topic: str, partitions: List[int]) -> Dict[int, int]:
        """Get stored offsets for topic partitions from PostgreSQL."""
        conn = psycopg2.connect(self.pg_settings.get_connection_string())

        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT partition_id, offset_value
                    FROM kafka_offsets
                    WHERE consumer_id = %s AND topic = %s AND partition_id = ANY(%s)
                    """,
                    (CONSUMER_ID, topic, partitions)
                )

                stored_offsets = {}
                for partition_id, offset_value in cursor.fetchall():
                    stored_offsets[partition_id] = offset_value

                logger.info(f"Retrieved stored offsets: {stored_offsets}")
                return stored_offsets

        finally:
            conn.close()

    def update_offsets(self, topic: str, partition_offsets: Dict[int, int]) -> None:
        """Update stored offsets in PostgreSQL."""
        if not partition_offsets:
            return

        conn = psycopg2.connect(self.pg_settings.get_connection_string())

        try:
            with conn.cursor() as cursor:
                for partition_id, offset_value in partition_offsets.items():
                    cursor.execute(
                        """
                        INSERT INTO kafka_offsets (consumer_id, topic, partition_id, offset_value, updated_at)
                        VALUES (%s, %s, %s, %s, NOW())
                        ON CONFLICT (consumer_id, topic, partition_id)
                        DO UPDATE SET
                            offset_value = EXCLUDED.offset_value,
                            updated_at = EXCLUDED.updated_at
                        """,
                        (CONSUMER_ID, topic, partition_id, offset_value)
                    )

                conn.commit()
                logger.info(f"Updated offsets: {partition_offsets}")

        except Exception as e:
            conn.rollback()
            logger.error(f"Error updating offsets: {e}")
            raise
        finally:
            conn.close()


def get_optimized_kafka_config(settings: KafkaSettings) -> dict:
    """Get optimized Kafka configuration for fast connections without consumer groups."""
    config = settings.get_kafka_config()

    # Optimized for speed - no consumer groups
    config.update({
        # Connection optimizations
        "request_timeout_ms": 10_000,       # Reasonable timeout
        "retry_backoff_ms": 100,            # Quick retries
        "reconnect_backoff_ms": 50,         # Fast reconnection

        # Fetching optimizations
        "fetch_min_bytes": 1,               # Don't wait for large batches
        "fetch_max_wait_ms": 500,           # Low latency
        "max_poll_records": 1000,           # Get more records per poll

        # Consumer behavior (no offset management by Kafka)
        "auto_offset_reset": "earliest",     # Fallback if seeking fails
        "enable_auto_commit": False,         # We manage offsets manually
        "consumer_timeout_ms": 3_000,        # Quick timeout for no messages

        "value_deserializer": lambda x: x.decode("utf-8"),
    })

    # Remove group_id to avoid consumer group coordination
    if "group_id" in config:
        del config["group_id"]

    return config


def get_quotes_sync(count: int = 1, env_file: Optional[str] = None) -> List[BondQuote]:
    """Synchronously get quotes with PostgreSQL offset tracking."""
    start_time = time.time()

    try:
        # Load settings
        if env_file:
            settings = KafkaSettings(_env_file=env_file)  # type: ignore
        else:
            settings = KafkaSettings()  # type: ignore

        kafka_config = get_optimized_kafka_config(settings)
        offset_manager = PostgresOffsetManager(env_file)

        logger.info(f"Creating Kafka consumer for {count} quotes...")
        consumer = KafkaConsumer(**kafka_config)

        # Get available partitions
        partitions = consumer.partitions_for_topic(settings.eventhub_name)
        if not partitions:
            logger.error(f"No partitions found for topic {settings.eventhub_name}")
            return []

        # Create topic partitions
        topic_partitions = [TopicPartition(settings.eventhub_name, p) for p in partitions]
        consumer.assign(topic_partitions)

        # Get stored offsets from PostgreSQL
        stored_offsets = offset_manager.get_stored_offsets(settings.eventhub_name, list(partitions))

        # Seek to stored offsets (or beginning if no stored offset)
        for tp in topic_partitions:
            if tp.partition in stored_offsets:
                seek_offset = stored_offsets[tp.partition]
                consumer.seek(tp, seek_offset)
                logger.info(f"Partition {tp.partition}: seeking to stored offset {seek_offset}")
            else:
                # No stored offset, start from beginning
                consumer.seek_to_beginning(tp)
                logger.info(f"Partition {tp.partition}: seeking to beginning (no stored offset)")

        connection_time = time.time() - start_time
        logger.info(f"Kafka connection and seeking completed in {connection_time:.2f}s")

        quotes = []
        poll_start = time.time()
        new_offsets = {}  # Track new offsets to store

        try:
            # Poll for messages
            while len(quotes) < count:
                message_batch = consumer.poll(timeout_ms=2000)

                if not message_batch:
                    # No more messages available
                    poll_time = time.time() - poll_start
                    logger.warning(f"No more messages available after {poll_time:.2f}s")
                    break

                # Process all messages in batch
                for topic_partition, messages in message_batch.items():
                    for message in messages:
                        if len(quotes) >= count:
                            break

                        try:
                            quote = BondQuote.model_validate_json(message.value)
                            quotes.append(quote)

                            # Track the offset for this partition (next message to read)
                            new_offsets[topic_partition.partition] = message.offset + 1

                            logger.debug(f"Received quote {len(quotes)}/{count}: {quote.ticker} "
                                       f"(partition {topic_partition.partition}, offset {message.offset})")

                        except Exception as e:
                            logger.error(f"Error parsing quote: {e}")
                            # Still update offset to skip bad messages
                            new_offsets[topic_partition.partition] = message.offset + 1
                            continue

                # Break if we have enough quotes
                if len(quotes) >= count:
                    break

        except KafkaTimeoutError:
            logger.warning("Kafka consumer timeout - no messages available")
        except Exception as e:
            logger.error(f"Error during message consumption: {e}")
            raise
        finally:
            # Always close the consumer
            try:
                consumer.close()
            except Exception as e:
                logger.error(f"Error closing consumer: {e}")

        # Update offsets in PostgreSQL for next request
        if new_offsets:
            offset_manager.update_offsets(settings.eventhub_name, new_offsets)

        total_time = time.time() - start_time
        logger.info(f"Retrieved {len(quotes)} quotes in {total_time:.2f}s")

        return quotes

    except Exception as e:
        total_time = time.time() - start_time
        logger.error(f"Error getting quotes after {total_time:.2f}s: {e}")
        raise


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle."""
    logger.info("Starting Progressive Quote API server with PostgreSQL offset tracking...")
    yield
    logger.info("Shutting down Progressive Quote API server...")


# FastAPI app
app = FastAPI(title="Progressive Bond Quote API", lifespan=lifespan)


@app.get("/quote")
async def get_single_quote():
    """Get a single quote with progressive consumption."""
    start_time = time.time()

    try:
        env_file = getattr(app.state, 'env_file', None)
        quotes = get_quotes_sync(count=1, env_file=env_file)

        response_time = time.time() - start_time

        if not quotes:
            logger.warning(f"No quotes available after {response_time:.2f}s")
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"No quotes available (response time: {response_time:.2f}s)"
            )

        logger.info(f"Served 1 quote in {response_time:.2f}s")
        return quotes[0]

    except HTTPException:
        raise
    except Exception as e:
        response_time = time.time() - start_time
        logger.error(f"Error serving quote after {response_time:.2f}s: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal server error: {str(e)}"
        )


@app.get("/quotes")
async def get_multiple_quotes(
    batch_size: int = Query(
        default=10,
        ge=1,
        le=10_000,
        description="Number of quotes to retrieve (1-10,000)",
    )
):
    """Get multiple quotes with progressive consumption."""
    start_time = time.time()

    try:
        env_file = getattr(app.state, 'env_file', None)
        quotes = get_quotes_sync(count=batch_size, env_file=env_file)

        response_time = time.time() - start_time

        if not quotes:
            logger.warning(f"No quotes available after {response_time:.2f}s")
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"No quotes available (response time: {response_time:.2f}s)"
            )

        logger.info(f"Served {len(quotes)} quotes in {response_time:.2f}s")
        return quotes

    except HTTPException:
        raise
    except Exception as e:
        response_time = time.time() - start_time
        logger.error(f"Error serving quotes after {response_time:.2f}s: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal server error: {str(e)}"
        )


@app.get("/health")
async def health_check():
    """Health check with connection test."""
    start_time = time.time()

    try:
        # Quick connection test
        env_file = getattr(app.state, 'env_file', None)
        quotes = get_quotes_sync(count=1, env_file=env_file)

        response_time = time.time() - start_time

        return {
            "api_status": "healthy",
            "kafka_connection": "ok",
            "postgres_offset_tracking": "ok",
            "test_quote_available": len(quotes) > 0,
            "connection_time_seconds": round(response_time, 2)
        }

    except Exception as e:
        response_time = time.time() - start_time
        return {
            "api_status": "degraded",
            "error": str(e),
            "connection_time_seconds": round(response_time, 2)
        }


@app.get("/offsets")
async def get_current_offsets():
    """Get current stored offsets for debugging."""
    try:
        env_file = getattr(app.state, 'env_file', None)

        if env_file:
            settings = KafkaSettings(_env_file=env_file)  # type: ignore
            pg_settings = PostgresSettings(_env_file=env_file)  # type: ignore
        else:
            settings = KafkaSettings()  # type: ignore
            pg_settings = PostgresSettings()  # type: ignore

        conn = psycopg2.connect(pg_settings.get_connection_string())

        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT partition_id, offset_value, updated_at
                    FROM kafka_offsets
                    WHERE consumer_id = %s AND topic = %s
                    ORDER BY partition_id
                    """,
                    (CONSUMER_ID, settings.eventhub_name)
                )

                offsets = []
                for partition_id, offset_value, updated_at in cursor.fetchall():
                    offsets.append({
                        "partition": partition_id,
                        "offset": offset_value,
                        "updated_at": updated_at.isoformat()
                    })

                return {
                    "consumer_id": CONSUMER_ID,
                    "topic": settings.eventhub_name,
                    "offsets": offsets
                }

        finally:
            conn.close()

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error retrieving offsets: {str(e)}"
        )


@app.post("/reset-offsets")
async def reset_offsets():
    """Reset all stored offsets (start over from beginning)."""
    try:
        env_file = getattr(app.state, 'env_file', None)

        if env_file:
            settings = KafkaSettings(_env_file=env_file)  # type: ignore
            pg_settings = PostgresSettings(_env_file=env_file)  # type: ignore
        else:
            settings = KafkaSettings()  # type: ignore
            pg_settings = PostgresSettings()  # type: ignore

        conn = psycopg2.connect(pg_settings.get_connection_string())

        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    DELETE FROM kafka_offsets
                    WHERE consumer_id = %s AND topic = %s
                    """,
                    (CONSUMER_ID, settings.eventhub_name)
                )

                deleted_count = cursor.rowcount
                conn.commit()

                return {
                    "message": "Offsets reset successfully",
                    "deleted_offsets": deleted_count,
                    "next_consumption": "will start from beginning"
                }

        finally:
            conn.close()

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error resetting offsets: {str(e)}"
        )


if __name__ == "__main__":
    # For local development
    import uvicorn
    from dotenv import load_dotenv

    env_file = sys.argv[1] if len(sys.argv) > 1 else ".env"
    load_dotenv(env_file)

    # Store env_file for the endpoints
    app.state.env_file = env_file

    logger.info(f"Starting progressive server with env file: {env_file}")
    uvicorn.run(app, host="0.0.0.0", port=8000)