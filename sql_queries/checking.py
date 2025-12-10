from config_sql import get_engine
import pandas as pd

engine = get_engine()

cols = pd.read_sql("DESCRIBE fact_measurement", engine)
print(cols)
