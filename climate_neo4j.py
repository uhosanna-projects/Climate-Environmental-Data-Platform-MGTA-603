import networkx as nx
import pandas as pd
from matplotlib import pyplot as plt
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

stations_query = """
MATCH (s:Station)
RETURN s.station_id AS station_id,
       s.station_name AS station_name,
       s.latitude AS latitude,
       s.longitude AS longitude
"""

stations_df = run_query(stations_query)
stations_df.head()

near_query = """
MATCH (a:Station)-[r:NEAR]-(b:Station)
RETURN a.station_id AS from_id,
       b.station_id AS to_id,
       r.distance_km AS distance_km
"""

near_df = run_query(near_query)
near_df.head()

G = nx.Graph()

# Add nodes (stations)
for _, row in stations_df.iterrows():
    G.add_node(
        row['station_id'],
        latitude=row['latitude'],
        longitude=row['longitude'],
        station_name=row['station_name']
    )

# Add edges (NEAR relationships)
for _, row in near_df.iterrows():
    G.add_edge(
        row['from_id'],
        row['to_id'],
        distance=row['distance_km']
    )

plt.figure(figsize=(12, 8))

# Get positions based on lat/lon
pos = {
    row['station_id']: (row['longitude'], row['latitude'])
    for _, row in stations_df.iterrows()
}

# Draw edges (NEAR links)
nx.draw_networkx_edges(G, pos, alpha=0.3)

# Draw stations
nx.draw_networkx_nodes(
    G, pos,
    node_size=40,
    node_color='blue',
    alpha=0.8
)

plt.title("Station Map with NEAR Relationships (Neo4j → Python)")
plt.xlabel("Longitude")
plt.ylabel("Latitude")
plt.grid(True)
plt.show()


