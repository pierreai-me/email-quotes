from datetime import date, datetime
from pydantic import BaseModel
from pydantic_settings import BaseSettings


class BondQuote(BaseModel):
    sender: str
    recipient: list[str]
    quote_timestamp: datetime
    ticker: str
    price: float
    coupon: float
    maturity: date


class KafkaSettings(BaseSettings):
    eventhub_connection_string: str
    eventhub_name: str
    kafka_broker: str

    model_config = {
        "env_file": ".env",
        "case_sensitive": False,
        "extra": "ignore",
    }

    def get_kafka_config(self) -> dict:
        """Get Kafka configuration for Azure Event Hubs"""

        return {
            "bootstrap_servers": [self.kafka_broker],
            "security_protocol": "SASL_SSL",
            "sasl_mechanism": "PLAIN",
            "sasl_plain_username": "$ConnectionString",
            "sasl_plain_password": self.eventhub_connection_string,
            "client_id": "bond-quote-client",
        }
