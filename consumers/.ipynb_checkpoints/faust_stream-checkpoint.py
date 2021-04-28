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

app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
#Topic
topic = app.topic("jdbc.stations", value_type=Station)
# Output Topics
out_topic = app.topic("faust.stations.transformed", partitions=1, value_type=TransformedStation)

table = app.Table(
    "stations.transformation.table",
    default=int,
    partitions=1,
    changelog_topic=out_topic,
)
@app.agent(topic)
async def StationProcess(stream):
    
    async for event in stream:     
        transformed_line = ""
        if(event.red == True):
            transformed_line = "red"
        elif(event.blue == True):
            transformed_line = "blue"
        elif(event.green == True):
            transformed_line = "green"
        else:
            transformed_line = "null"
        
        table[event.station_id] = TransformedStation(         
            station_id=event.station_id,
            station_name=event.station_name,
            order=event.order,
            line=transformed_line)


if __name__ == "__main__":
    app.main()
