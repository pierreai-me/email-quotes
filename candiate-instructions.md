# Candidate instructions

## Architecture

```
+-------------------------------------------------------------------------------------+
|                                                                                     |
| +---------------+     +---------------+     +---------------+     +---------------+ |
| |  DATA SOURCES |     |  PROCESSING   |     | DATA STORAGE  |     |   QUERYING    | |
| |  (PROVIDED)   |     |  (CANDIDATE)  |     |   (MIXED)     |     |  (CANDIDATE)  | |
| +---------------+     +---------------+     +---------------+     +---------------+ |
|                                                                                     |
| +---------------+     +---------------+     +---------------+     +---------------+ |
| | Kafka Queue   |     |  Ingestion    |     |  PostgreSQL   |     | Query API     | |
| |               |     |               |     |               |     |               | |
| |               |     |               |     | * Server: [P] |     |               | |
| |               |     |               |     | * Schema: [C] |     |               | |
| |               |     |               |     | * Data:   [C] |     |               | |
| |               |     |               |     |               |     |               | |
| +---------------+     +---------------+     +---------------+     +---------------+ |
|                                                                                     |
| +---------------+                           +---------------+                       |
| | REST API      |                           |   Cosmos DB   |                       |
| |               |                           |               |                       |
| | * GET /quote  |                           | * Account:[P] |                       |
| | * GET /quotes |                           | * Schema: [C] |                       |
| |               |                           | * Data:   [C] |                       |
| |               |                           |               |                       |
| +---------------+                           +---------------+                       |
|                                                                                     |
|                                             +---------------+                       |
|                                             | SQLite        |                       |
|                                             |               |                       |
|                                             | * File:   [C] |                       |
|                                             | * Schema: [C] |                       |
|                                             | * Data:   [C] |                       |
|                                             |               |                       |
|                                             +---------------+                       |
|                                                                                     |
+-------------------------------------------------------------------------------------+
```

Legend:
- `[P]` = Provided by us
- `[C]` = Implemented by candidate

## Candidate tasks

1. Data ingestion
    * Choose: Kafka consumer or REST API client
    * Parse BondQuote JSON messages
    * Handle errors
    * (Advanced) Improve latency and throughput
2. Database design
    * Choose: PostgreSQL, Cosmos DB, or SQLite
    * Design schema
    * Create tables / collections
3. Data storage
    * Insert parsed quotes into chosen database
    * (Advanced) Improve latency and throughput
4. Query interface (likely won't have time)
    * Query all quotes and display them
    * (Advanced) Filter by different criteria

## Data model

```
BondQuote {
    sender: string              // "bloomberg@bloomberg.com"
    recipient: string[]         // ["trader1@fund.com", "desk@bank.com"]
    quote_timestamp: datetime   // When quote was generated
    ticker: string              // "AAPL", "GOOGL", etc.
    price: float                // Bond price
    coupon: float               // Interest rate %
    maturity: date              // When bond matures
}
```