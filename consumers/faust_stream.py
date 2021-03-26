"""Defines trends calculations for stations"""
import logging

import faust


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str

def transform(station: Station) -> TransformedStation:
    line = ""
    try: 
        line ="red" if station.red else "blue" if station.blue else "green" if station.green else None
        if line is None:
            raise ValueError("Line cannot be None.")
    except ValueError:
        logger.error('Station lines were all false!')
    transformed_station = TransformedStation(station.station_id, station.station_name, station.order, line)
    return transformed_station

app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
in_topic = app.topic("com.cta.analytics.db.stations", value_type=Station)
out_topic = app.topic("com.cta.analytics.streams.stations", partitions=1)

# TODO: Define a Faust Table
table = app.Table(
   "stations",
   default=int,
   partitions=1,
   changelog_topic=out_topic,
)


@app.agent(in_topic)
async def db_station(stations):

    async for station in stations:
        transformedStation = transform(station)
        await out_topic.send(key=transformedStation.station_name, value=transformedStation)


if __name__ == "__main__":
    app.main()
