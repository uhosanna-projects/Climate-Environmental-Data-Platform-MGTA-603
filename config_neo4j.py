import os

from dotenv import load_dotenv

load_dotenv()

user = os.getenv('NEO4J_USER', 'neo4j')
password = os.getenv('NEO4J_PASSWORD', '')
host = os.getenv('NEO4J_HOST', '127.0.0.1')
port = int(os.getenv('NEO4J_PORT', '7687'))