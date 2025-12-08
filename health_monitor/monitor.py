import socket
import time
import json
import logging
import threading
import docker
import os
import hashlib

# Configuration constants
DEFAULT_MONITOR_PORT = 5000
DEFAULT_HEARTBEAT_TIMEOUT = 5.0  # seconds without heartbeat to consider node dead
DEFAULT_CHECK_INTERVAL = 1.0  # interval to check for dead nodes
DEFAULT_UDP_BUFFER_SIZE = 1024  # bytes
DEFAULT_BIND_ADDRESS = '0.0.0.0'

class HealthMonitor:
    def __init__(self, hostname, monitors, monitored_services=None, port=DEFAULT_MONITOR_PORT, timeout=DEFAULT_HEARTBEAT_TIMEOUT, check_interval=DEFAULT_CHECK_INTERVAL):
        """
        Args:
            hostname (str): Name of this health monitor (e.g., health-monitor-1).
            monitors (list): List of all health monitor hostnames, sorted.
            monitored_services (list): List of all services to monitor (workers + monitors).
            port (int): UDP port to listen on.
            timeout (float): Seconds without heartbeat to consider a node dead.
            check_interval (float): Interval to check for dead nodes.
        """
        self.hostname = hostname
        self.monitors = sorted(monitors)
        self.monitored_services = monitored_services or []
        self.port = port
        self.timeout = timeout
        self.check_interval = check_interval
        self.udp_buffer_size = DEFAULT_UDP_BUFFER_SIZE
        
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((DEFAULT_BIND_ADDRESS, self.port))
        
        self.last_heartbeats = {}
        self.lock = threading.Lock()
        self.running = False
        
        # Initialize heartbeats for all monitored services
        # Set timestamp to 0 to force immediate check after restart
        now = time.time()
        for service in self.monitored_services:
            self.last_heartbeats[service] = 0 if service != self.hostname else now
        
        # Ensure all monitors are in the list (backward compatibility)
        for m in self.monitors:
            if m not in self.last_heartbeats:
                self.last_heartbeats[m] = 0 if m != self.hostname else now

        try:
            self.docker_client = docker.from_env()
        except Exception as e:
            logging.error(f"Failed to connect to Docker Daemon: {e}")
            self.docker_client = None

    def start(self):
        self.running = True
        
        # Start UDP listener thread
        self.listener_thread = threading.Thread(target=self._listen_udp, daemon=True)
        self.listener_thread.start()
        
        # Start Monitor loop
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()
        
        logging.info(f"HealthMonitor {self.hostname} started. Monitors: {self.monitors}")

    def _listen_udp(self):
        logging.info(f"Listening for heartbeats on port {self.port}")
        while self.running:
            try:
                data, addr = self.sock.recvfrom(self.udp_buffer_size)
                message = json.loads(data.decode('utf-8'))
                service_name = message.get('service_name')
                timestamp = message.get('timestamp')
                
                if service_name and timestamp:
                    with self.lock:
                        self.last_heartbeats[service_name] = time.time()
                        #logging.debug(f"Received heartbeat from {service_name}")
                        
            except Exception as e:
                logging.error(f"Error receiving heartbeat: {e}")

    def _monitor_loop(self):
        while self.running:
            time.sleep(self.check_interval)
            self._check_dead_nodes()

    def _get_active_monitors(self):
        """
        Returns a sorted list of monitors that are currently considered alive.
        """
        active = []
        now = time.time()
        with self.lock:
            for m in self.monitors:
                if m == self.hostname:
                    active.append(m)
                    continue
                
                last_seen = self.last_heartbeats.get(m)
                if last_seen and (now - last_seen <= self.timeout):
                    active.append(m)
        return sorted(active)

    def _check_dead_nodes(self):
        now = time.time()
        dead_nodes = []
        
        with self.lock:
            for service, last_seen in self.last_heartbeats.items():
                if now - last_seen > self.timeout:
                    dead_nodes.append(service)
        
        active_monitors = self._get_active_monitors()
        #logging.debug(f"Active monitors: {active_monitors}")

        dead_monitors = [node for node in dead_nodes if node in self.monitors]
        dead_workers = [node for node in dead_nodes if node not in self.monitors]
        
        # Priority 1: Revive monitors
        for node in dead_monitors:
            if self._am_i_responsible(node, active_monitors):
                logging.warning(f"CRITICAL: Monitor {node} is DEAD. Reviving with HIGH PRIORITY (Active Ring: {len(active_monitors)})...")
                self._revive_node(node)
        
        # Priority 2: Revive workers
        for node in dead_workers:
            if self._am_i_responsible(node, active_monitors):
                logging.info(f"Node {node} is DEAD. I am responsible (Active Ring: {len(active_monitors)}). Reviving...")
                self._revive_node(node)
            else:
                pass

    def _am_i_responsible(self, service_name, active_monitors):
        if not active_monitors:
            return False
            
        h = int(hashlib.md5(service_name.encode('utf-8')).hexdigest(), 16)
        index = h % len(active_monitors)
        responsible_monitor = active_monitors[index]
        
        return responsible_monitor == self.hostname

    def _revive_node(self, container_name):
        if not self.docker_client:
            logging.error("Docker client not available. Cannot revive.")
            return

        try:            
            container = self.docker_client.containers.get(container_name)
            
            if container.status != 'running':
                logging.info(f"Restarting container {container_name}...")
                container.restart()
                logging.info(f"Container {container_name} restarted.")
            else:
                logging.info(f"Container {container_name} is already running (maybe false positive or just recovered).")
                
        except docker.errors.NotFound:
            logging.error(f"Container {container_name} not found.")
        except Exception as e:
            logging.error(f"Error reviving {container_name}: {e}")

    def stop(self):
        self.running = False
        self.sock.close()
