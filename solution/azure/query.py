#!/usr/bin/env python3
"""
Flexible query client for bond quotes system.

Supports querying both SQL (PostgreSQL) and NoSQL (Cosmos DB) databases
with various operators and efficient pagination for large datasets.

Usage examples:
    python query.py --sql --ticker AAPL
    python query.py --no-sql --timestamp ">=2025-01-10_09:10:13" --timestamp "<=2025-02-01_00:00:00" --coupon "<3.55"
    python query.py --sql --sender "bloomberg@bloomberg.com" --price ">100.0"
    python query.py --no-sql --recipient "trader1@hedge-fund.com" --maturity ">=2025-06-01"
    python query.py --sql --limit 50 --offset 100
"""

import argparse
import re
import psycopg2
from azure.cosmos import CosmosClient
from datetime import datetime, date
from typing import List, Dict, Any, Tuple, Optional
from solution.azure.models import PostgresSettings, CosmosSettings


class QueryParser:
    """Parse and validate query criteria with operators."""
    VALID_FIELDS = [
        "sender",
        "recipient",
        "quote_timestamp",
        "ticker",
        "price",
        "coupon",
        "maturity",
    ]

    def __init__(self):
        self.criteria = []

    def _parse_field_value(self, field: str, value: str) -> Tuple[str, str, Any]:
        """Parse field value with optional operator prefix."""
        # Extract operator from the beginning of the value
        operator_match = re.match(r"^(>=|<=|!=|>|<|=)", value)
        if operator_match:
            operator = operator_match.group(1)
            raw_value = value[len(operator) :]
        else:
            operator = "="
            raw_value = value
        converted_value = self._convert_value(field, raw_value)
        return field, operator, converted_value

    def _convert_value(self, field: str, value: str) -> Any:
        """Convert string value to appropriate type based on field."""
        if field in ["price", "coupon"]:
            return float(value)
        elif field == "quote_timestamp":
            # Handle timestamp format with underscores
            timestamp_str = value.replace("_", " ")
            return datetime.fromisoformat(timestamp_str)
        elif field == "maturity":
            return datetime.fromisoformat(value).date()
        else:
            return value

    def add_criteria(self, field: str, value: str):
        """Add a query criterion."""
        if field not in self.VALID_FIELDS:
            raise ValueError(
                f"Invalid field: {field}. Valid fields: {self.VALID_FIELDS}"
            )
        field, operator, converted_value = self._parse_field_value(field, value)
        self.criteria.append((field, operator, converted_value))


class SqlQueryExecutor:

    def __init__(self, env_file: str):
        self.settings = PostgresSettings(_env_file=env_file)  # type: ignore
        self.conn = psycopg2.connect(self.settings.get_connection_string())

    def execute_query(
        self,
        criteria: List[Tuple[str, str, Any]],
        limit: Optional[int] = None,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        query, params = self._build_sql_query(criteria, limit, offset)

        with self.conn.cursor() as cur:
            cur.execute(query, params)

            # Get column names
            columns = [desc[0] for desc in cur.description]  # type: ignore

            results = []
            for row in cur.fetchall():
                result = dict(zip(columns, row))
                results.append(result)

            return results

    def _build_sql_query(
        self,
        criteria: List[Tuple[str, str, Any]],
        limit: Optional[int] = None,
        offset: int = 0,
    ) -> Tuple[str, List[Any]]:
        base_query = """
        SELECT q.id, q.sender, q.quote_timestamp, q.ticker, q.price, q.coupon, q.maturity,
                array_agg(qr.recipient ORDER BY qr.recipient) as recipients
        FROM quotes q
        LEFT JOIN quote_recipients qr ON q.id = qr.quote_id
        """
        where_clauses = []
        params = []

        for field, operator, value in criteria:
            if field == "recipient":
                # Special handling for recipient field (requires join)
                where_clauses.append(f"qr.recipient {operator} %s")
                params.append(value)
            else:
                # Map field names to actual column names
                column_name = f"q.{field}"
                where_clauses.append(f"{column_name} {operator} %s")
                params.append(value)

        if where_clauses:
            base_query += " WHERE " + " AND ".join(where_clauses)

        base_query += " GROUP BY q.id, q.sender, q.quote_timestamp, q.ticker, q.price, q.coupon, q.maturity"
        base_query += " ORDER BY q.quote_timestamp DESC"

        if limit:
            base_query += f" LIMIT {limit}"
        if offset:
            base_query += f" OFFSET {offset}"

        return base_query, params

    def close(self):
        self.conn.close()


class CosmosQueryExecutor:
    """Execute queries against Cosmos DB."""

    def __init__(self, env_file: str):
        self.settings = CosmosSettings(_env_file=env_file)  # type: ignore
        self.client = CosmosClient(
            self.settings.cosmos_endpoint, self.settings.cosmos_key
        )
        self.database = self.client.get_database_client(self.settings.cosmos_database)
        self.container = self.database.get_container_client(
            self.settings.cosmos_container
        )

    def execute_query(
        self,
        criteria: List[Tuple[str, str, Any]],
        limit: Optional[int] = None,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        query, parameters = self._build_cosmos_query(criteria, limit, offset)
        items = self.container.query_items(
                query=query, parameters=parameters, enable_cross_partition_query=True
            )
        return list(items)

    def _build_cosmos_query(
        self,
        criteria: List[Tuple[str, str, Any]],
        limit: Optional[int] = None,
        offset: int = 0,
    ) -> Tuple[str, List[Dict[str, Any]]]:
        query = "SELECT * FROM c"
        where_clauses = []
        parameters = []

        for i, (field, operator, value) in enumerate(criteria):
            param_name = f"@param{i}"

            if field == "recipient":
                if operator == "=":
                    where_clauses.append(f"ARRAY_CONTAINS(c.recipients, {param_name})")
                else:
                    where_clauses.append(
                        f"EXISTS(SELECT VALUE r FROM r IN c.recipients WHERE r {operator} {param_name})"
                    )
            elif field == "quote_timestamp":
                value_str = (
                    value.isoformat() if isinstance(value, datetime) else str(value)
                )
                where_clauses.append(f"c.quote_timestamp {operator} {param_name}")
                value = value_str
            elif field == "maturity":
                value_str = value.isoformat() if isinstance(value, date) else str(value)
                where_clauses.append(f"c.maturity {operator} {param_name}")
                value = value_str
            else:
                where_clauses.append(f"c.{field} {operator} {param_name}")

            parameters.append({"name": param_name, "value": value})

        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)

        query += " ORDER BY c.quote_timestamp DESC"

        if offset:
            query += f" OFFSET {offset}"
        if limit:
            query += f" LIMIT {limit}"

        return query, parameters

    def close(self):
        pass


def format_result(result: Dict[str, Any]) -> str:
    """Format a single result for display."""
    recipients = result.get("recipients", [])
    if recipients == [None]:
        recipients = []
    return (
        f"ID:{result['id']} | {result['sender']} -> {recipients} | "
        f"{result['quote_timestamp']} | {result['ticker']} ${result['price']} "
        f"{result['coupon']}% {result['maturity']}"
    )


def main():
    parser = argparse.ArgumentParser()

    # Database selection
    db_group = parser.add_mutually_exclusive_group(required=True)
    db_group.add_argument("--sql", action="store_true", help="Query SQL database")
    db_group.add_argument("--no-sql", action="store_true", help="Query NoSQL database")

    # Environment file
    parser.add_argument("--env-file", required=True, help="Path to .env file")

    # Query fields
    parser.add_argument("--sender", action="append", help="Sender email (=, !=, LIKE)")
    parser.add_argument("--recipient", action="append", help="Recipient email (=, !=)")
    parser.add_argument(
        "--timestamp", action="append", help="Quote timestamp (=, !=, >, <, >=, <=)"
    )
    parser.add_argument("--ticker", action="append", help="Stock ticker (=, !=)")
    parser.add_argument(
        "--price", action="append", help="Bond price (=, !=, >, <, >=, <=)"
    )
    parser.add_argument(
        "--coupon", action="append", help="Coupon rate (=, !=, >, <, >=, <=)"
    )
    parser.add_argument(
        "--maturity", action="append", help="Maturity date (=, !=, >, <, >=, <=)"
    )

    # Pagination
    parser.add_argument("--limit", type=int, help="Maximum number of results to return")
    parser.add_argument(
        "--offset", type=int, default=0, help="Number of results to skip"
    )

    args = parser.parse_args()

    # Parse query criteria
    query_parser = QueryParser()

    for field in [
        "sender",
        "recipient",
        "timestamp",
        "ticker",
        "price",
        "coupon",
        "maturity",
    ]:
        values = getattr(args, field)
        if values:
            for value in values:
                # Map 'timestamp' back to 'quote_timestamp' for internal use
                actual_field = "quote_timestamp" if field == "timestamp" else field
                query_parser.add_criteria(actual_field, value)

    executor = (
        SqlQueryExecutor(args.env_file)
        if args.sql
        else CosmosQueryExecutor(args.env_file)
    )
    print(f"Executor: {executor}")

    results = executor.execute_query(query_parser.criteria, args.limit, args.offset)

    print(f"Found {len(results)} results:")
    print("-" * 80)

    for result in results:
        print(format_result(result))

    executor.close()


if __name__ == "__main__":
    exit(main())
