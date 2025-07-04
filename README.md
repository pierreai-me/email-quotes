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
export RESOURCE_GROUP="emailquotes001"
export ENV_FILE="./cloud/${RESOURCE_GROUP}/${RESOURCE_GROUP}.env"

time make -j azure RESOURCE_GROUP=${RESOURCE_GROUP}
# make delete-azure RESOURCE_GROUP=${RESOURCE_GROUP}

python -m solution.azure.create_sql --env-file "$ENV_FILE"
python -m solution.azure.kafka_producer --env-file "$ENV_FILE" --count 15
python -m solution.azure.kafka_consumer --env-file "$ENV_FILE" --count 10 --insert-sql --insert-no-sql
python -m solution.azure.show_sql --env-file "$ENV_FILE"
python -m solution.azure.show_no_sql --env-file "$ENV_FILE"
python -m solution.azure.query --env-file "$ENV_FILE" --sql --ticker AAPL

# Run system tests
python -m pytest -s tests/system/test_azure.py

# Run & query local server
python -m solution.azure.quote_server "$ENV_FILE"
curl http://localhost:8000/quotes?batch_size=10

# Query remote server
curl https://${RESOURCE_GROUP}api.azurewebsites.net/quote
```

## Goal

This is a long form (2-hour) AI interview question that deals with databases and REST APIs.
We are providing the candidate with the following cloud components:
1. A Kafka queue containing bond quotes addressed to some recipients. It is constantly populated with data.
2. A REST API that provides a GET /quote endpoint which returns the next quote available
3. An empty SQL database.
4. An empty NoSQL database.
5. A mechanism to always replenish the Kafka queue.

In practice, candidates will either use the Kafka queue (1) or the REST API (2) to get data.
We are providing both to make sure candidates can use the technology they are most comfortable with.
We can even provide a flat file for candidates who are comfortable with neither.

Also, they will use a database to implement their solution.
They can either use a SQL database (3) or a No SQL database (4) depending on their preferences and the constraints of the problem, for instance the load.

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

## Observations

### Operations performance

| Operation | Condition | Quotes | Time (s) | Rate (msg/s) |
| --- | --- | --- | --- | --- |
| Kafka W | No batch | 1,000 | 46.5 | 21.5 |
| Kafka W | Batch | 10,000 | 0.8 | 11,890 |
| Kafka R |  | 10,000 | 10.9 | 917 |
| Kafka R, SQL W | Batch 1 | 200 | 28.6 | 7 |
| Kafka R, SQL W | Batch 10,000 | 10,000 | 11.5 | 867 |
| Kafka R, NoSQL W | Batch 1 | 1,000 | 39.3 | 25.5 |
| Kafka R, NoSQL W | Batch 10,000 | 10,000 | 26.9 | 372 |

### Bugs & limitations

- When sending quotes to Kafka, if gzip compression is enabled, the producer completes successfully but the consumer does not see any quotes
- When aborting the consumer, rerunning again does not pick up new messages (I have to use a different consumer group) (these 2 issues might be related)
- There is an intermittent failure when querying Cosmos DB using --coupon '>3.0'
- The FastAPI server takes 5-10 seconds to connect to the Kafka queue every time, making it impractical to GET /queue.
