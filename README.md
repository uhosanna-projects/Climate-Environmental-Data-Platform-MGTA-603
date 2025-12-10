# 🌦️ Climate-Environmental-Data-Platform-MGTA-603

This project implements an end-to-end polyglot data platform for analyzing Canadian climate data using MySQL, MongoDB, Neo4j, Redis, and Streamlit.
The system supports data ingestion, cleaning, transformation, storage, aggregation, and visualization.

📌 Project Overview
The platform demonstrates how different data models can be used together for environmental analytics:
-SQL (MySQL): stores normalized daily climate measurements using a star schema.
-MongoDB: stores monthly aggregated documents for fast analytical queries.
-Neo4j: models spatial relationships between weather stations for geospatial insights.
-Redis: caches climate leaderboards (hottest/wettest stations).
-Streamlit: provides a unified interactive dashboard.

The project includes a full ETL workflow, database integration, analytical queries, and visualizations.

🧱 System Architecture 
MySQL (Structured Warehouse)
Tables: fact_measurement, dim_station, dim_date, dim_sensor
Supports rainfall heatmaps, temperature variability, and stored procedure analytics
Includes stored procedure: get_monthly_rainfall_for_year(year)
MongoDB (Monthly Aggregates): Stores computed monthly averages

Used for queries like “Top 3 warmest months”
Neo4j (Graph Analytics)
Nodes = stations
Edges = NEAR (within 100 km)
Used to visualize regional climate clusters
Redis (Leaderboards)
Cached hottest/wettest stations
Used for fast dashboard loading
Streamlit (Dashboard)

Runs SQL, MongoDB, Neo4j, and Redis queries

Provides visual insights (heatmaps, boxplots, graphs, leaderboards)

⚙️ How to Run the Project
1. Install dependencies
2. Ensure all services are running
3. Launch the dashboard(streamlit run streamlit_app.py)


⭐ Key Features

End-to-end ETL pipeline for climate data
Multi-database architecture with polyglot persistence
Stored procedure integration from Streamlit
Rainfall heatmaps, temperature variability plots, and boxplots
MongoDB monthly summaries and insights
Neo4j graph visualization of station proximity
Redis climate leaderboards (hottest/wettest stations)
Interactive Streamlit dashboard

