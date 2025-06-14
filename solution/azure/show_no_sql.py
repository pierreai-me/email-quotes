#!/usr/bin/env python3
import argparse
from azure.cosmos import CosmosClient
from solution.azure.models import CosmosSettings


def show_quotes(env_file: str):
    settings = CosmosSettings(_env_file=env_file)  # type: ignore
    client = CosmosClient(settings.cosmos_endpoint, settings.cosmos_key)
    database = client.get_database_client(settings.cosmos_database)
    container = database.get_container_client(settings.cosmos_container)

    query = """
    SELECT * FROM c
    ORDER BY c.quote_timestamp DESC
    """

    items = container.query_items(query=query, enable_cross_partition_query=True)

    for item in items:
        recipients = item.get("recipients", [])
        print(
            f"ID:{item['id']} | {item['sender']} -> {recipients} | "
            f"{item['quote_timestamp']} | {item['ticker']} ${item['price']} "
            f"{item['coupon']}% {item['maturity']}"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--env-file", required=True, help="Path to .env file")
    show_quotes(parser.parse_args().env_file)
