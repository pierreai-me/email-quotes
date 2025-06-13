#!/usr/bin/env python3
"""
Database schema creation script for bond quotes system using PostgreSQL.

Usage:
    python create_database.py --env-file ./cloud/emailquotes001/emailquotes001.env
"""

import argparse
import psycopg2


from pydantic_settings import BaseSettings


class DatabaseSettings(BaseSettings):
    """Database connection settings from environment file."""

    postgres_host: str
    postgres_database: str
    postgres_username: str
    postgres_password: str
    postgres_port: str = "5432"
    postgres_sslmode: str = "require"

    model_config = {
        "env_file": ".env",
        "case_sensitive": False,
        "extra": "ignore",
    }


def create_database_schema(settings: DatabaseSettings) -> None:
    # Build connection string
    conn_string = f"host={settings.postgres_host} dbname={settings.postgres_database} user={settings.postgres_username} password={settings.postgres_password} port={settings.postgres_port} sslmode={settings.postgres_sslmode}"

    print(f"Connecting to PostgreSQL database...")
    conn = psycopg2.connect(conn_string)
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

        print("Creating indices...")
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_quotes_timestamp ON quotes (quote_timestamp)"
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
    settings = DatabaseSettings(_env_file=args.env_file) # type: ignore
    create_database_schema(settings)


if __name__ == "__main__":
    main()
