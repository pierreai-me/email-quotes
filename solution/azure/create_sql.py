#!/usr/bin/env python3
"""
Database schema creation script for bond quotes system using PostgreSQL.

Includes offset tracking for progressive consumption in quote_server.py.

Usage:
    python create_database.py --env-file ./cloud/emailquotes001/emailquotes001.env
"""

import argparse
import psycopg2
from solution.azure.models import SqlSettings, PostgresSettings


def create_database_schema(settings: SqlSettings) -> None:
    print(f"Connecting to PostgreSQL database...")
    conn = psycopg2.connect(settings.get_connection_string())
    cursor = conn.cursor()

    try:
        print("Creating quotes table...")
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS quotes (
                id BIGSERIAL PRIMARY KEY,
                sender VARCHAR(255) NOT NULL,
                quote_timestamp TIMESTAMP NOT NULL,
                ticker VARCHAR(10) NOT NULL,
                price DECIMAL(10,4) NOT NULL,
                coupon DECIMAL(5,3) NOT NULL,
                maturity DATE NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        print("Creating quote_recipients table...")
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS quote_recipients (
                id BIGSERIAL PRIMARY KEY,
                quote_id BIGINT NOT NULL,
                recipient VARCHAR(255) NOT NULL
            )
            """
        )

        print("Creating kafka_offsets table...")
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS kafka_offsets (
                id SERIAL PRIMARY KEY,
                consumer_id VARCHAR(255) NOT NULL,
                topic VARCHAR(255) NOT NULL,
                partition_id INTEGER NOT NULL,
                offset_value BIGINT NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(consumer_id, topic, partition_id)
            )
            """
        )

        print("Creating indices...")
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_quotes_timestamp ON quotes (quote_timestamp)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_kafka_offsets_lookup ON kafka_offsets (consumer_id, topic, partition_id)"
        )

        print("Adding foreign key constraint...")
        cursor.execute(
            """
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.table_constraints
                    WHERE constraint_name = 'fk_quote_recipients_quote_id'
                ) THEN
                    ALTER TABLE quote_recipients
                    ADD CONSTRAINT fk_quote_recipients_quote_id
                    FOREIGN KEY (quote_id) REFERENCES quotes(id) ON DELETE CASCADE;
                END IF;
            END $$
        """
        )

        conn.commit()
        print("Database schema created successfully")

    except Exception as e:
        print(f"Error creating database schema: {type(e)} {e}")
        conn.rollback()
        raise

    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="Create database schema for bond quotes system"
    )
    parser.add_argument(
        "--env-file",
        required=True,
        help="Path to .env file containing database connection settings",
    )
    args = parser.parse_args()
    settings = PostgresSettings(_env_file=args.env_file)  # type: ignore
    create_database_schema(settings)


if __name__ == "__main__":
    main()