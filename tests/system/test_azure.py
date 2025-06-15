import os
import time
from datetime import datetime
from typing import Tuple
from solution.azure.kafka_consumer import consume_quotes
from solution.azure.kafka_producer import produce_quotes
from solution.azure.show_sql import get_quotes as get_sql_quotes
from solution.azure.show_no_sql import get_quotes as get_nosql_quotes
from solution.azure.models import BondQuote


def normalize_quote(quote: BondQuote) -> Tuple:
    """Convert BondQuote to comparable tuple."""
    return (
        quote.sender,
        tuple(sorted(quote.recipient)),
        quote.quote_timestamp,
        quote.ticker,
        quote.price,
        quote.coupon,
        quote.maturity,
    )


def normalize_sql_row(row: Tuple) -> Tuple:
    """Convert SQL row to comparable tuple."""
    recipients = tuple(sorted(row[7])) if row[7] != [None] else ()
    return (row[1], recipients, row[2], row[3], float(row[4]), float(row[5]), row[6])


def normalize_nosql_item(item: dict) -> Tuple:
    """Convert NoSQL item to comparable tuple."""
    recipients = tuple(sorted(item.get("recipients", [])))
    return (
        item["sender"],
        recipients,
        datetime.fromisoformat(item["quote_timestamp"]),
        item["ticker"],
        item["price"],
        item["coupon"],
        datetime.fromisoformat(item["maturity"]).date(),
    )


def test_quote_pipeline_system():
    # Get env file
    env_file = os.environ["ENV_FILE"]
    n_quotes = 1_000

    print("Produce quotes")
    production_start_time = time.time()
    produced_quotes = produce_quotes(env_file, count=n_quotes, batch=True)
    production_time = time.time() - production_start_time

    print("Consume quotes")
    start_time = time.time()
    consumed_quotes = consume_quotes(
        env_file=env_file,
        count=n_quotes,
        insert_sql=True,
        insert_cosmos=True,
        batch_size=2 * n_quotes,
        commit_interval=5,
        start_timestamp=production_start_time - 1,
    )
    consumption_time = time.time() - start_time

    print("Verify consumed quotes contain all produced quotes")
    produced_normalized = {normalize_quote(q) for q in produced_quotes}
    consumed_normalized = {normalize_quote(q) for q in consumed_quotes}
    assert produced_normalized.issubset(consumed_normalized), (
        f"Consumed quotes don't match produced. "
        f"Produced: {len(produced_normalized)}, Consumed: {len(consumed_normalized)}"
    )

    print("Verify SQL database contains all produced quotes")
    sql_rows = list(get_sql_quotes(env_file))
    sql_normalized = {normalize_sql_row(row) for row in sql_rows}
    assert produced_normalized.issubset(sql_normalized), (
        f"SQL missing quotes. Produced: {len(produced_normalized)}, "
        f"SQL: {len(sql_normalized)}, Missing: {len(produced_normalized - sql_normalized)}, "
        f"First produced: {next(iter(produced_normalized), 'None')}, "
        f"First SQL: {next(iter(sql_normalized), 'None')}"
    )

    print("Verify NoSQL database contains all produced quotes")
    nosql_items = list(get_nosql_quotes(env_file))
    nosql_normalized = {normalize_nosql_item(item) for item in nosql_items}
    assert produced_normalized.issubset(nosql_normalized), (
        f"NoSQL missing quotes. Produced: {len(produced_normalized)}, "
        f"NoSQL: {len(nosql_normalized)}, Missing: {len(produced_normalized - nosql_normalized)}"
    )

    print("Verify performance requirements")
    assert (
        production_time <= 1.0
    ), f"Production took {production_time:.2f} sec, must be <=≤ 1.0 sec"
    consumption_rate = len(consumed_quotes) / consumption_time
    assert (
        consumption_rate >= 200
    ), f"Consumption rate {consumption_rate:.1f} quotes / sec, must be >= 200 / sec"
