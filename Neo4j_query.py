from neo4j import GraphDatabase
from matplotlib import pyplot as plt
import networkx as nx
from ETL_pipelines.climate_neo4j import run_query
from config_neo4j import host, port, user, password

uri = f"neo4j://{host}:{port}"
driver = GraphDatabase.driver(uri, auth=(user, password))


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


