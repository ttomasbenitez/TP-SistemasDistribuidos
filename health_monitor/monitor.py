import socket
import time
import json
import logging
import threading
import docker
import os
import hashlib

class HealthMonitor:
    def __init__(self, hostname, monitors, port=5000, timeout=5.0, check_interval=1.0):
        """
        Args:
            hostname (str): Name of this health monitor (e.g., health-monitor-1).
            monitors (list): List of all health monitor hostnames, sorted.
            port (int): UDP port to listen on.
            timeout (float): Seconds without heartbeat to consider a node dead.
            check_interval (float): Interval to check for dead nodes.
        """
        self.hostname = hostname
        self.monitors = sorted(monitors)
        self.port = port
        self.timeout = timeout
        self.check_interval = check_interval
        
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(('0.0.0.0', self.port))
        
        self.last_heartbeats = {}
        self.lock = threading.Lock()
        self.running = False
        
        # Initialize heartbeats for all monitors to avoid startup race conditions
        # We assume everyone is alive at start (optimistic)
        now = time.time()
        for m in self.monitors:
            self.last_heartbeats[m] = now

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
                data, addr = self.sock.recvfrom(1024)
                message = json.loads(data.decode('utf-8'))
                service_name = message.get('service_name')
                timestamp = message.get('timestamp')
                
                if service_name and timestamp:
                    with self.lock:
                        self.last_heartbeats[service_name] = time.time()
                        logging.debug(f"Received heartbeat from {service_name}")
                        
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
        logging.debug(f"Active monitors: {active_monitors}")

        for node in dead_nodes:
            if self._am_i_responsible(node, active_monitors):
                logging.info(f"Node {node} is DEAD. I am responsible (Active Ring: {len(active_monitors)}). Reviving...")
                self._revive_node(node)
                with self.lock:
                    self.last_heartbeats[node] = now
            else:
                pass

    def _am_i_responsible(self, service_name, active_monitors):
        """
        Ring algorithm:
        Hash the service name and find the monitor with the next highest hash.
        For simplicity, since we have a fixed list of monitors, we can use consistent hashing 
        or just modulo arithmetic on the hash.
        
        Simple approach: hash(service_name) % len(active_monitors) -> index of responsible monitor.
        """
        if not active_monitors:
            return False
            
        # Use a stable hash (MD5)
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
