from influxdb_client import InfluxDBClient
from datetime import datetime, timedelta
import pandas as pd

INFLUXDB_URL = "http://influxdb-influxdb.apps.tenoran.automation.otic.open6g.net"
INFLUXDB_TOKEN = ""
INFLUXDB_ORG = "wines"
INFLUXDB_BUCKET = ""

def query_recent_metrics():
    client = InfluxDBClient(
        url=INFLUXDB_URL,
        token=INFLUXDB_TOKEN,
        org=INFLUXDB_ORG
    )
    
    query_api = client.query_api()
    
    query = f"""
    from(bucket: "{INFLUXDB_BUCKET}")
        |> range(start: -3s)
        |> filter(fn: (r) => r["_measurement"] == "ue_metrics")
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    """
    
    try:
        tables = query_api.query(query)
        
        print("\nRecent Metrics (last 3 seconds):")
        for table in tables:
            for record in table.records:
                print(f"\nTimestamp: {record.get_time()}")
                for key, value in record.values.items():
                    if key not in ['_start', '_stop', '_measurement']:
                        print(f"{key}: {value}")
                
    except Exception as e:
        print(f"Error querying data: {str(e)}")
    finally:
        client.close()

if __name__ == "__main__":
    query_recent_metrics()