

import pandas as pd
from sqlalchemy import create_engine, text

from config_sql import get_engine

engine = get_engine()


# query = """
# CALL get_monthly_rainfall_for_year(2024)"""
#
#
# monthly_rainfall = pd.read_sql_query(query, engine)
#
# # 4) Look at the result
# print(monthly_rainfall.head())

# Use raw MySQL connection to avoid "commands out of sync"
conn = engine.raw_connection()
cursor = conn.cursor()

try:
    # Call stored procedure
    cursor.callproc("get_monthly_rainfall_for_year", [2024])

    # MySQL stored procedures return results through stored_results()
    for result in cursor.stored_results():
        rows = result.fetchall()
        columns = [col[0] for col in result.description]
        df = pd.DataFrame(rows, columns=columns)

finally:
    cursor.close()
    conn.close()

print(df)