import os
import time
import logging
from monitor import HealthMonitor
from utils.heartbeat import HeartbeatSender
from monitor import DEFAULT_MONITOR_PORT, DEFAULT_HEARTBEAT_TIMEOUT, DEFAULT_CHECK_INTERVAL

def initialize_log(logging_level):
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging_level,
        datefmt='%Y-%m-%d %H:%M:%S',
    )

def main():
    logging_level = os.getenv('LOG_LEVEL', 'INFO')
    initialize_log(logging_level)
    
    hostname = os.getenv('HOSTNAME')
    node_name = os.getenv('NODE_NAME')
    
    monitors_str = os.getenv('HEALTH_MONITORS', '')
    monitors = [m.strip() for m in monitors_str.split(',') if m.strip()]
    
    if not node_name:
        logging.error("NODE_NAME env var is missing.")
        return

    if not monitors:
        logging.error("HEALTH_MONITORS env var is missing.")
        return

    logging.info(f"Starting Health Monitor: {node_name}")
        
    # Read configuration from environment variables
    port = int(os.getenv('MONITOR_PORT', DEFAULT_MONITOR_PORT))
    timeout = float(os.getenv('MONITOR_TIMEOUT', DEFAULT_HEARTBEAT_TIMEOUT))
    check_interval = float(os.getenv('MONITOR_CHECK_INTERVAL', DEFAULT_CHECK_INTERVAL))
    
    # Read monitored services list
    monitored_services_str = os.getenv('MONITORED_SERVICES', '')
    monitored_services = [s.strip() for s in monitored_services_str.split(',') if s.strip()]
    
    if monitored_services:
        logging.info(f"Monitoring {len(monitored_services)} services: {monitored_services}")
    else:
        logging.warning("MONITORED_SERVICES env var not set. Will only track services that send heartbeats.")
    
    monitor = HealthMonitor(
        hostname=node_name,
        monitors=monitors,
        monitored_services=monitored_services,
        port=port,
        timeout=timeout,
        check_interval=check_interval
    )
    monitor.start()

    # Start sending heartbeats too, so other monitors know I'm alive
    # We use the same node_name as service_name
    sender = HeartbeatSender(node_name, monitors)
    sender.start()
    
    # Keep main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        monitor.stop()
        logging.info("Health Monitor stopped.")

if __name__ == "__main__":
    main()