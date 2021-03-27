"""Configures KSQL to combine station and turnstile data"""
import json
import logging

import requests

import topic_check

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


KSQL_URL = "http://localhost:8088"


KSQL_STATEMENT = """
CREATE TABLE turnstile (
    station_id int,
    station_name string,
    line string
) WITH (
    kafka_topic='org.chicago.cta.trunstile',
    value_format='AVRO',
    key='station_id'
);

CREATE TABLE turnstile_summary
WITH (kafka_topic='TURNSTILE_SUMMARY', value_format='JSON') AS
    SELECT station_id, COUNT(station_id) as count
    FROM turnstile
    GROUP BY station_id ;
"""


def execute_statement():
    """Executes the KSQL statement against the KSQL API"""
    if topic_check.topic_exists("TURNSTILE_SUMMARY"):
        return

    logging.debug("executing ksql statement...")

    resp = requests.post(
        f"{KSQL_URL}/ksql",
        headers={"Content-Type": "application/vnd.ksql.v1+json"},
        data=json.dumps( 
            { 
                "ksql": KSQL_STATEMENT,
                "streamsProperties": {"ksql.streams.auto.offset.reset": "earliest"}
            }
        )
    )

    # Ensure that a 2XX status code was returned
    resp.raise_for_status()
    logger.debug(resp)


if __name__ == "__main__":
    execute_statement()
