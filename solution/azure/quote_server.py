#!/usr/bin/env python3
"""
Provides REST endpoints to consume bond quotes from Kafka:
- GET /quote - returns a single quote
- GET /quotes?batch_size=N - returns multiple quotes (default 10, max 10,000)

This server uses a different consumer group than kafka_consumer.py to avoid
conflicts and ensure independent consumption.
"""

import sys
from contextlib import asynccontextmanager
from fastapi import FastAPI, Response, Query, Response, status
from kafka import KafkaConsumer
from .models import BondQuote, KafkaSettings


def get_quotes(count: int = 1) -> list[BondQuote]:
    # Load settings from environment
    settings = KafkaSettings()  # type: ignore
    kafka_config = settings.get_kafka_config()
    kafka_config.update(
        {
            "auto_offset_reset": "earliest",
            "enable_auto_commit": True,
            "auto_commit_interval_ms": 1_000,
            "group_id": "bond-quote-server-consumer",
            "value_deserializer": lambda x: x.decode("utf-8"),
            "consumer_timeout_ms": 30_000,
            "fetch_min_bytes": 1,
            "fetch_max_wait_ms": 1_000,
            "max_poll_records": 10_000,
        }
    )
    consumer = KafkaConsumer(settings.eventhub_name, **kafka_config)

    print(f"Initialized Kafka consumer for topic: {settings.eventhub_name}")
    print(
        f"Consuming {count} bond quotes from topic {settings.eventhub_name} "
        f"broker {settings.kafka_broker}..."
    )

    ret = []

    try:
        for message in consumer:
            try:
                quote = BondQuote.model_validate_json(message.value)
                ret.append(quote)
                print(f"Received quote {len(ret)}/{count}: {quote}")
                if len(ret) >= count:
                    break

            except Exception as e:
                print(f"Error processing message {message.value}: {type(e)} {e}")
                continue

    except Exception as e:
        print(f"Error processing messages: {type(e)} {e}")

    finally:
        consumer.close()

    return ret


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle."""
    print("Starting Quote API server...")
    yield
    print("Shutting down Quote API server...")


# FastAPI app
app = FastAPI(title="Bond Quote API", lifespan=lifespan)


@app.get("/quote")
async def get_single_quote():
    quotes = get_quotes(count=1)
    return quotes[0] if quotes else Response(status_code=status.HTTP_204_NO_CONTENT)


@app.get("/quotes")
async def get_multiple_quotes(
    batch_size: int = Query(
        default=10,
        ge=1,
        le=10_000,
        description="Number of quotes to retrieve (1-10,000)",
    )
):
    return get_quotes(count=batch_size)


@app.get("/health")
async def health_check():
    return {"api_status": "healthy"}


if __name__ == "__main__":
    # For local development
    import uvicorn
    from dotenv import load_dotenv

    load_dotenv(sys.argv[1])
    uvicorn.run(app, host="0.0.0.0", port=8000)
