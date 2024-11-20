import re
import json
import time
import os
import logging
from datetime import datetime
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# InfluxDB configuration
INFLUXDB_URL = "http://influxdb-influxdb.apps.tenoran.automation.otic.open6g.net"
INFLUXDB_TOKEN = ""
INFLUXDB_ORG = "wines"
INFLUXDB_BUCKET = ""

def send_to_influx(client, metrics):
    """Send metrics to InfluxDB with enhanced error handling and logging"""
    write_api = client.write_api(write_options=SYNCHRONOUS)
    
    if not metrics:
        logger.error("No metrics to send")
        return
        
    logger.debug(f"Attempting to send metrics: {json.dumps(metrics, indent=2)}")
    
    try:
        for ue_id, ue_data in metrics.items():
            # Validate data before sending - check if values are not None
            if any(v is None for v in ue_data['downlink'].values()) or any(v is None for v in ue_data['uplink'].values()):
                logger.warning(f"Incomplete metrics for UE {ue_id}")
                continue

            # Downlink metrics
            dl = ue_data['downlink']
            dl_point = Point("ue_metrics")\
                .tag("ue_id", ue_id)\
                .tag("direction", "downlink")\
                .field("rsrp", dl['RSRP'])\
                .field("cqi", dl['CQI'])\
                .field("bler", dl['BLER'])\
                .field("mcs", dl['MCS'])\
                .field("bytes", dl['transmitted_bytes'])
            
            try:
                write_api.write(bucket=INFLUXDB_BUCKET, record=dl_point)
                logger.debug(f"Successfully sent downlink metrics for UE {ue_id}")
            except Exception as e:
                logger.error(f"Failed to send downlink metrics for UE {ue_id}: {str(e)}")

            # Uplink metrics
            ul = ue_data['uplink']
            ul_point = Point("ue_metrics")\
                .tag("ue_id", ue_id)\
                .tag("direction", "uplink")\
                .field("bler", ul['BLER'])\
                .field("mcs", ul['MCS'])\
                .field("snr", ul['SNR'])\
                .field("bytes", ul['transmitted_bytes'])
            
            try:
                write_api.write(bucket=INFLUXDB_BUCKET, record=ul_point)
                logger.debug(f"Successfully sent uplink metrics for UE {ue_id}")
            except Exception as e:
                logger.error(f"Failed to send uplink metrics for UE {ue_id}: {str(e)}")

    except Exception as e:
        logger.error(f"Error in send_to_influx: {str(e)}")
        raise
        
def verify_influxdb_connection(client):
    """Verify InfluxDB connection and permissions"""
    try:
        buckets_api = client.buckets_api()
        bucket = buckets_api.find_bucket_by_name(INFLUXDB_BUCKET)
        if not bucket:
            logger.error(f"Bucket '{INFLUXDB_BUCKET}' not found")
            return False
        logger.info(f"Successfully connected to InfluxDB and found bucket '{INFLUXDB_BUCKET}'")
        return True
    except Exception as e:
        logger.error(f"Failed to verify InfluxDB connection: {str(e)}")
        return False


def parse_ue_metrics(file_path):
    """
    Parse UE metrics from a log file with enhanced debugging
    """
    metrics = {}
    current_ue = None
    
    # Updated regex patterns
    ue_id_pattern = re.compile(r'UE RNTI (\w+)')
    rsrp_pattern = re.compile(r'average RSRP (-?\d+)')
    cqi_pattern = re.compile(r'UE \w+: CQI (\d+)')
    dl_pattern = re.compile(r'dlsch_rounds \d+/\d+/\d+/\d+.*BLER ([\d.]+) MCS.*?(\d+)')
    # Updated uplink pattern to match your log format
    ul_pattern = re.compile(r'ulsch_rounds (\d+)/(\d+)/(\d+)/(\d+).*BLER ([\d.]+) MCS.*?(\d+)')
    bytes_pattern = re.compile(r'UE \w+: MAC:\s+TX\s+(\d+)\s+RX\s+(\d+)\s+bytes')
    
    try:
        with open(file_path, 'r') as file:
            content = file.read()
            logger.debug(f"File content:\n{content}")
            
            for line in content.splitlines():
                line = line.strip()
                logger.debug(f"Processing line: {line}")
                
                ue_match = ue_id_pattern.search(line)
                if ue_match:
                    current_ue = ue_match.group(1)
                    logger.debug(f"Found UE ID: {current_ue}")
                    if current_ue not in metrics:
                        metrics[current_ue] = {
                            'downlink': {
                                'RSRP': None,
                                'CQI': None,
                                'BLER': None,
                                'MCS': None,
                                'transmitted_bytes': None
                            },
                            'uplink': {
                                'BLER': None,
                                'MCS': None,
                                'PRB': 0,  # Default value since not in log
                                'SNR': 0,  # Default value since not in log
                                'transmitted_bytes': None
                            }
                        }
                    
                    rsrp_match = rsrp_pattern.search(line)
                    if rsrp_match:
                        value = int(rsrp_match.group(1))
                        logger.debug(f"Found RSRP: {value}")
                        metrics[current_ue]['downlink']['RSRP'] = value
                
                elif current_ue:
                    if 'CQI' in line:
                        cqi_match = cqi_pattern.search(line)
                        if cqi_match:
                            value = int(cqi_match.group(1))
                            logger.debug(f"Found CQI: {value}")
                            metrics[current_ue]['downlink']['CQI'] = value
                    
                    elif 'dlsch_rounds' in line:
                        dl_match = dl_pattern.search(line)
                        if dl_match:
                            bler = float(dl_match.group(1))
                            mcs = int(dl_match.group(2))
                            logger.debug(f"Found downlink BLER: {bler}, MCS: {mcs}")
                            metrics[current_ue]['downlink']['BLER'] = bler
                            metrics[current_ue]['downlink']['MCS'] = mcs
                    
                    elif 'ulsch_rounds' in line:
                        ul_match = ul_pattern.search(line)
                        if ul_match:
                            bler = float(ul_match.group(5))
                            mcs = int(ul_match.group(6))
                            logger.debug(f"Found uplink BLER: {bler}, MCS: {mcs}")
                            metrics[current_ue]['uplink']['BLER'] = bler
                            metrics[current_ue]['uplink']['MCS'] = mcs
                    
                    elif 'MAC:' in line:
                        bytes_match = bytes_pattern.search(line)
                        if bytes_match:
                            tx = int(bytes_match.group(1))
                            rx = int(bytes_match.group(2))
                            logger.debug(f"Found bytes TX: {tx}, RX: {rx}")
                            metrics[current_ue]['downlink']['transmitted_bytes'] = tx
                            metrics[current_ue]['uplink']['transmitted_bytes'] = rx

        logger.debug(f"Final parsed metrics: {json.dumps(metrics, indent=2)}")
        return metrics
    except Exception as e:
        logger.error(f"Error parsing file: {str(e)}")
        return None

def monitor_file(file_path, influx_client, check_interval=1):
    """Monitor file for changes with enhanced debugging"""
    last_modified = None
    logger.info(f"Monitoring {file_path} for changes")
    
    try:
        while True:
            try:
                current_modified = os.path.getmtime(file_path)
                if last_modified != current_modified:
                    logger.info(f"File modified at {datetime.fromtimestamp(current_modified)}")
                    metrics = parse_ue_metrics(file_path)
                    if metrics:
                        logger.info("Successfully parsed metrics, sending to InfluxDB")
                        send_to_influx(influx_client, metrics)
                    else:
                        logger.warning("No metrics parsed from file")
                    last_modified = current_modified
                time.sleep(check_interval)
            except FileNotFoundError:
                logger.warning(f"File {file_path} not found")
                time.sleep(check_interval)
    except KeyboardInterrupt:
        logger.info("Stopping file monitor")

def main():
    file_path = 'nrMAC_stats.log'
    
    logger.info(f"Connecting to InfluxDB at {INFLUXDB_URL}")
    client = InfluxDBClient(
        url=INFLUXDB_URL,
        token=INFLUXDB_TOKEN,
        org=INFLUXDB_ORG
    )
    
    if not verify_influxdb_connection(client):
        logger.error("Failed to verify InfluxDB connection. Exiting.")
        return

    try:
        monitor_file(file_path, client)
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
    finally:
        client.close()

if __name__ == "__main__":
    main()