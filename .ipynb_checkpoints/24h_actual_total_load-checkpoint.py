#Import Libraries
import logging
import pandas as pd
import os
import random
import time, datetime
from entsoe import EntsoePandasClient
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.client.query_api import QueryApi

# Set up logging
logging.basicConfig(filename='24h_entsoe_actual_total_load.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.info('Script started at %s', pd.Timestamp.now(tz='UTC'))

# Set API key
API_KEY = '6276342c-e10c-4d88-8688-cb0a1cf163ca'

# Define InfluxDB connection details
influxdb_url = 'http://159.89.103.242:8086'
influxdb_token = 'dfRMxqDtwyHK7vDJHelAm0WKISLvKFUrmhclvaaAoMFOHRRTGNnYkV8bXd0jR9r4arvkg3l_lWNSHyKMG0WxSg=='
influxdb_org = 'entra'
influxdb_bucket = 'entra'

# Initialize the Entsoe client
client = EntsoePandasClient(api_key=API_KEY)

# Create InfluxDB client
influx_client = InfluxDBClient(url=influxdb_url, token=influxdb_token, org=influxdb_org)

# Create the write API
write_api = influx_client.write_api(write_options=SYNCHRONOUS)
# Instantiate the query API
query_api = QueryApi(influx_client)

# Define the time period
start = pd.Timestamp.now(tz='UTC') - pd.Timedelta(days=1)
end = pd.Timestamp.now(tz='UTC')

# Define the categories and their corresponding measurements
categories = {
    '6.1.A': 'actual_total_load',
    '6.1.B': 'day_ahead_total_load_forecast',
    '14.1.A': 'installed_generation_capacity',
    '16.1.B&C': 'actual_generation'
}

# Country codes
country_codes = ["DE_50HZ", "IT_NORD_SI", "AL", "IT_PRGP", "DE_AMPRION", "IT_ROSN", "AT", "IT_SARD", "BY", "IT_SICI",
                 "BE", "IT_SUD", "BA", "RU_KGD", "BG", "LV", "CZ_DE_SK", "LT", "HR", "LU", "CWE", "MT", "CY", "ME",
                 "CZ", "GB", "DE_AT_LU", "NL", "DE_LU", "NO_1", "DK", "NO_2", "DK_1", "NO_3", "DK_2", "NO_4", "DK_CA",
                 "NO_5", "EE", "NO", "FI", "PL_CZ", "MK", "PL", "FR", "PT", "DE", "MD", "GR", "RO", "HU", "RU", "IS",
                 "SE_1", "IE_SEM", "SE_2", "IE", "SE_3", "IT", "SE_4", "IT_SACO_AC", "RS", "IT_SACO_DC", "SK", "IT_BRNN",
                 "SI", "IT_CNOR", "GB_NIR", "IT_CSUD", "ES", "IT_FOGN", "SE", "IT_GR", "CH", "IT_MACRO_NORTH",
                 "DE_TENNET", "IT_MACRO_SOUTH", "DE_TRANSNET", "IT_MALTA", "TR", "IT_NORD", "UA", "IT_NORD_AT",
                 "UA_DOBTPP", "IT_NORD_CH", "UA_BEI", "IT_NORD_FR", "UA_IPS"]

sorted_country_codes = sorted(country_codes)
failed_actual_total_load=[]


#Actual Total Load - Query and Upload Data for each country into the InfluxDB
for country_code in sorted_country_codes:
    try:
        # Download the actual total load data
        actual_total_load = client.query_load(country_code=country_code, start=start, end=end)
        print(f"Actual Total Load for {country_code}:")
        print(actual_total_load)  # Print the data

        # Rename the columns
        actual_total_load.columns = ['value']

        # Convert the index to UTC and format it as string
        actual_total_load.index = actual_total_load.index.tz_convert('UTC').strftime('%Y-%m-%dT%H:%M:%SZ')

        # Convert the data to InfluxDB Line Protocol format
        data_points = []
        for timestamp, value in actual_total_load.iterrows():
            data_point = Point("actual_total_load") \
                .tag("country", country_code) \
                .field("value", value['value']) \
                .time(timestamp, WritePrecision.NS)
            data_points.append(data_point)
            
        write_api.write(bucket=influxdb_bucket, record=data_points)
        logging.info('Data extraction (actual_total_load) completed for country: %s', country_code)
    except Exception as e:
        failed_actual_total_load.append(country_code)
        logging.error(f'Data extraction (actual_total_load) failed for country: {country_code}: {str(e)}')
        
logging.info('Script ended at %s', pd.Timestamp.now(tz='UTC'))
print(failed_actual_total_load)