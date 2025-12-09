import pandas as pd
from neo4j import GraphDatabase
from config_neo4j import host, port, user, password
from config_sql import get_engine



uri = f"neo4j://{host}:{port}"
driver = GraphDatabase.driver(uri, auth=(user, password))

def upload_station(tx, station_id, station_name, lat, lon):
    query = """
    MERGE (s:Station {station_id: $station_id})
    SET s.station_name = $station_name,
        s.latitude = $lat,
        s.longitude = $lon
    """
    tx.run(query, station_id=station_id, station_name=station_name,
           lat=float(lat), lon=float(lon))

def run_query(query, params=None):
    with driver.session() as session:
        result = session.run(query, params or {})
        return pd.DataFrame([r.data() for r in result])

engine=get_engine()
df = pd.read_sql_table("dim_station", engine)

with driver.session() as session:
    for _, row in df.iterrows():
        session.execute_write(
            upload_station,
            str(row['station_id']),
            row['station_name'],
            row['latitude'],
            row['longitude']
        )

print("Data upload completed ✔")

query = """
MATCH (s1:Station), (s2:Station)
WHERE id(s1) < id(s2)
WITH s1, s2,
     point({latitude:s1.latitude, longitude:s1.longitude}) AS p1,
     point({latitude:s2.latitude, longitude:s2.longitude}) AS p2
WITH s1, s2, point.distance(p1,p2)/1000 AS km
WHERE km < 100  // set your radius threshold
MERGE (s1)-[:NEAR {distance_km:km}]->(s2)
"""

run_query(query)

print("NEAR relationships created ✔")

