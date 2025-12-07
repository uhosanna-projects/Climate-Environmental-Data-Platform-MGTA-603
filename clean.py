import pandas as pd
import numpy as np

def clean(df):
    stations_to_drop = [
        "CASTLEGAR A",
        "FT STEELE DANDY CRK",
        "KINDAKUN ROCKS (AUT)",
        "LILLOOET",
        "MALAHAT",
        "LENNARD ISLAND"
    ]
    df = df[~df['STATION_NAME'].isin(stations_to_drop)]
    df = df.copy()
    df['LOCAL_DATE'] = pd.to_datetime(df['LOCAL_DATE'])
    return df

def sensors_data(df):
    # --- Dim_Sensor (UPDATED WITH ALL COLUMNS) ---
    sensors_data = {
        'sensor_id': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13],
        'sensor_name': [
            'Mean Temperature', 'Total Precipitation', 'Total Snow',
            'Min Temperature', 'Max Temperature', 'Total Rain', 'Snow on Ground',
            'Direction Max Gust', 'Speed Max Gust',
            'Cooling Degree Days', 'Heating Degree Days',
            'Min Relative Humidity', 'Max Relative Humidity'
        ],
        'unit': [
            'C', 'mm', 'cm',
            'C', 'C', 'mm', 'cm',
            '10s deg', 'km/h',
            'Index', 'Index',
            '%', '%'
        ]
    }
    dim_sensor = pd.DataFrame(sensors_data)
    return dim_sensor


def weather_data(df):
    # --- Dim_WeatherType ---
    weather_data = {
        'weather_type_id': [1, 2, 3, 4],
        'label': ['Sunny/Clear', 'Rainy', 'Snowy', 'Extreme Cold'],
        'description': ['No precipitation', 'Precipitation > 0mm', 'Snow > 0cm', 'Temperature < -20C']
    }
    dim_weather = pd.DataFrame(weather_data)
    return dim_weather


def stations_data(df):
    df['CLIMATE_IDENTIFIER'] = df['CLIMATE_IDENTIFIER'].astype(str)
    dim_station = df[['CLIMATE_IDENTIFIER', 'STATION_NAME', 'PROVINCE_CODE', 'y', 'x']].drop_duplicates(subset=['CLIMATE_IDENTIFIER'])
    dim_station.columns = ['station_id', 'station_name', 'province_code', 'latitude', 'longitude']
    return dim_station

def date_data(df):
    unique_dates = df['LOCAL_DATE'].drop_duplicates().sort_values()
    dim_date = pd.DataFrame({'full_date': unique_dates})
    dim_date['full_date'] = pd.to_datetime(dim_date['full_date'])
    dim_date['date_key'] = dim_date['full_date'].dt.strftime('%Y%m%d').astype(int)
    dim_date['year'] = dim_date['full_date'].dt.year
    dim_date['month'] = dim_date['full_date'].dt.month
    dim_date['month_name'] = dim_date['full_date'].dt.month_name()
    dim_date['day_of_week'] = dim_date['full_date'].dt.dayofweek + 1

    def get_season(month):
        if month in [12, 1, 2]:
            return 'Winter'
        elif month in [3, 4, 5]:
            return 'Spring'
        elif month in [6, 7, 8]:
            return 'Summer'
        else:
            return 'Fall'

    dim_date['season'] = dim_date['month'].apply(get_season)
    dim_date['is_weekend'] = dim_date['day_of_week'].isin([6, 7])
    dim_date = dim_date[['date_key', 'full_date', 'year', 'month', 'month_name', 'season', 'is_weekend']]
    return dim_date


def fact_measures(df):
    # MAP ALL CSV COLUMNS TO SENSOR IDs
    column_to_id_map = {
        'MEAN_TEMPERATURE': 1,
        'TOTAL_PRECIPITATION': 2,
        'TOTAL_SNOW': 3,
        'MIN_TEMPERATURE': 4,
        'MAX_TEMPERATURE': 5,
        'TOTAL_RAIN': 6,
        'SNOW_ON_GROUND': 7,
        'DIRECTION_MAX_GUST': 8,
        'SPEED_MAX_GUST': 9,
        'COOLING_DEGREE_DAYS': 10,
        'HEATING_DEGREE_DAYS': 11,
        'MIN_REL_HUMIDITY': 12,
        'MAX_REL_HUMIDITY': 13
    }

    # Identify which columns actually exist in your dataframe
    cols_to_keep = ['CLIMATE_IDENTIFIER', 'LOCAL_DATE'] + list(column_to_id_map.keys())
    existing_cols = [c for c in cols_to_keep if c in df.columns]

    # Unpivot (Melt)
    melted = df[existing_cols].melt(
        id_vars=['CLIMATE_IDENTIFIER', 'LOCAL_DATE'],
        var_name='sensor_csv_name',
        value_name='value'
    )

    # Remove nulls
    melted = melted.dropna(subset=['value'])

    # Map to Sensor ID
    melted['sensor_id'] = melted['sensor_csv_name'].map(column_to_id_map)

    # Generate Date Key
    melted['date_key'] = pd.to_datetime(melted['LOCAL_DATE']).dt.strftime('%Y%m%d').astype(int)

    # Create WIZARD-READY Fact Table (using blanks for NULLs)
    fact_table = pd.DataFrame()
    fact_table['station_id'] = melted['CLIMATE_IDENTIFIER']
    fact_table['date_key'] = melted['date_key']
    fact_table['sensor_id'] = melted['sensor_id']
    fact_table['value'] = melted['value']

    # Add Headers for Wizard
    fact_table.columns = ['station_id', 'date_key', 'sensor_id', 'value']
    return fact_table



