#!/usr/bin/env python3
import argparse
import psycopg2
from solution.azure.models import PostgresSettings


def get_quotes(env_file: str):
    """Generator that yields quote data from PostgreSQL database."""
    settings = PostgresSettings(_env_file=env_file)  # type: ignore
    conn = psycopg2.connect(settings.get_connection_string())

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT q.id, q.sender, q.quote_timestamp, q.ticker, q.price, q.coupon, q.maturity,
                   array_agg(qr.recipient ORDER BY qr.recipient) as recipients
            FROM quotes q
            LEFT JOIN quote_recipients qr ON q.id = qr.quote_id
            GROUP BY q.id, q.sender, q.quote_timestamp, q.ticker, q.price, q.coupon, q.maturity
            ORDER BY q.quote_timestamp DESC
        """
        )

        for row in cur.fetchall():
            yield row


def print_quotes(quotes):
    """Print quotes from an iterable of quote data."""
    for row in quotes:
        recipients = row[7] if row[7] != [None] else []
        print(
            f"ID:{row[0]} | {row[1]} -> {recipients} | {row[2]} | {row[3]} ${row[4]} {row[5]}% {row[6]}"
        )


def show_quotes(env_file: str):
    """Get quotes from database and print them."""
    print_quotes(get_quotes(env_file))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--env-file", required=True)
    show_quotes(parser.parse_args().env_file)
