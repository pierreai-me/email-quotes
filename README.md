# email-quotes
Cloud installation of the 2-hour Email Quotes interview question

## Local development

Setup:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Test:

```bash
RESOURCE_GROUP="emailquotes002"
ENV_FILE="./cloud/${RESOURCE_GROUP}/${RESOURCE_GROUP}.env"

time make -j azure RESOURCE_GROUP=${RESOURCE_GROUP}
# make delete-azure RESOURCE_GROUP=${RESOURCE_GROUP}

python -m solution.azure.create_sql --env-file "$ENV_FILE"
python -m solution.azure.kafka_producer --env-file "$ENV_FILE" --count 15
python -m solution.azure.kafka_consumer --env-file "$ENV_FILE" --count 10 --insert-sql --insert-no-sql
python -m solution.azure.show_sql --env-file "$ENV_FILE"
python -m solution.azure.show_no_sql --env-file "$ENV_FILE"
```

## Goal

This is a long form (2-hour) AI interview question that deals with databases and REST APIs. We are providing the candidate with the following cloud components:
1. A Kafka queue containing bond quotes addressed to some recipients. It is constantly populated with data.
2. A REST API that provides a GET /quote endpoint which returns the next quote available
3. An empty SQL database.
4. An empty NoSQL database.
5. A mechanism to always replenish the Kafka queue.

In practice, candidates will either use the Kafka queue (1) or the REST API (2) to get data. We are providing both to make sure candidates can use the technology they are ost comfortable with. We can even provide a flat file for candidates who are comfortable with neither.

Also, they will use a database to implement their solution. They can either use a SQL database (3) or a No SQL database (4) depending on their preferences and the constraints of the problem, for instance the load.

The goal of the question is to have the candidate implement a system in which they receive quotes, process them, store them, and allow querying them based on some attributes (initially a time range and a recipient).

Bond quotes provided by the Kafka queue or the REST API are simple strucures:

```py
class BondQuote(BaseModel):
    sender: str
    recipient: list[str]
    quote_timestamp: datetime.datetime
    ticker: str
    price: float  # actually decimal
    coupon: float
    maturity: datetime.date
```

## Prompts

### Getting started

Explain how all 5 components can be provided by AWS, Azure, and GCP. I'll be paying for those out of pocket so also compare prices. I'll be deleting all components after each interview (i.e. the setup will be active for only a few hours). Consider security -- how can I make sure the candidate can only access these components and no unrelated components that I own on these cloud providers.