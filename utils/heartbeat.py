import socket
import time
import json
import threading
import logging
import os

class HeartbeatSender:
    def __init__(self, service_name, health_monitors, port=5000, interval=1.0):
        """
        Args:
            service_name (str): Name of the service sending heartbeats.
            health_monitors (list): List of hostnames of health monitors.
            port (int): UDP port to send heartbeats to.
            interval (float): Interval in seconds between heartbeats.
        """
        self.service_name = service_name
        self.health_monitors = health_monitors
        self.port = port
        self.interval = interval
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.running = False
        self.thread = None

    def start(self):
        if self.running:
            return
        self.running = True
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()
        logging.info(f"HeartbeatSender started for {self.service_name}")

    def stop(self):
        self.running = False
        if self.thread:
            self.thread.join()
        logging.info("HeartbeatSender stopped")

    def _run(self):
        while self.running:
            try:
                payload = {
                    "service_name": self.service_name,
                    "timestamp": time.time()
                }
                message = json.dumps(payload).encode('utf-8')
                
                for monitor in self.health_monitors:
                    try:
                        self.sock.sendto(message, (monitor, self.port))
                    except Exception as e:
                        logging.debug(f"Failed to send heartbeat to {monitor}: {e}")
                        
            except Exception as e:
                logging.error(f"Error in HeartbeatSender: {e}")
            
            time.sleep(self.interval)

def start_heartbeat_sender():
    """
    Helper function to initialize and start the HeartbeatSender based on env vars.
    """
    service_name = os.getenv("CONTAINER_NAME")
    health_monitors_str = os.getenv("HEALTH_MONITORS", "")
    
    if not service_name:
        logging.warning(f"CONTAINER_NAME env var not set. HeartbeatSender not started. Env: {os.environ}")
        return None

    if not health_monitors_str:
        logging.warning(f"HEALTH_MONITORS env var not set. HeartbeatSender not started. Env: {os.environ}")
        return None

    health_monitors = [h.strip() for h in health_monitors_str.split(",") if h.strip()]
    
    logging.info(f"Initializing HeartbeatSender for {service_name} with monitors {health_monitors}")
    sender = HeartbeatSender(service_name, health_monitors)
    sender.start()
    return sender
