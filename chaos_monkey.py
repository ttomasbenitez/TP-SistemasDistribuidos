import argparse
import subprocess
import time
import random
import sys

EXCLUDED_CONTAINERS = ['rabbitmq', 'gateway', 
                       'filter-year-1', 
                       'filter-year-2', 
                       'filter-year-eof-service',
                       'client_1', 'client_2', 
                       'aggregator-month-eof-service',
                       'joiner-batches-2', 'health-monitor-1', 'health-monitor-2', 
                       'health-monitor-3']

def get_running_containers():
    """Returns a list of running container names for the current project."""
    try:
        # We use docker compose ps to get services defined in the current project
        # This ensures we don't kill random system containers
        result = subprocess.run(
            ['docker', 'compose', '-f', 'docker-compose-dev.yaml', 'ps', '--services', '--status', 'running'],
            capture_output=True,
            text=True,
            check=True
        )
        services = result.stdout.strip().split('\n')
        return [s for s in services if s]
    except subprocess.CalledProcessError as e:
        print(f"Error getting containers: {e}")
        return []

def kill_container(container_name):
    """Kills a specific container."""
    print(f"💥 Killing container: {container_name}")
    try:
        # docker compose kill takes the service name
        subprocess.run(
            ['docker', 'compose', '-f', 'docker-compose-dev.yaml', 'kill', container_name],
            check=True
        )
        print(f"💀 Container {container_name} killed.")
    except subprocess.CalledProcessError as e:
        print(f"Error killing container {container_name}: {e}")

def run_random_chaos(interval):
    """Kills a random container every interval seconds."""
    print(f"😈 Starting Chaos Monkey (Random Mode). Interval: {interval}s")
    print(f"🛡️  Excluded services: {EXCLUDED_CONTAINERS}")
    
    while True:
        containers = get_running_containers()
        # Filter out excluded
        targets = [c for c in containers if c not in EXCLUDED_CONTAINERS]
        
        if not targets:
            print("No eligible containers to kill.")
        else:
            target = random.choice(targets)
            kill_container(target)
        
        print(f"Sleeping for {interval} seconds...")
        time.sleep(interval)

def main():
    parser = argparse.ArgumentParser(description='Chaos Monkey for Docker Compose')
    parser.add_argument('--random', action='store_true', help='Enable random killing mode')
    parser.add_argument('--interval', type=int, default=2, help='Interval in seconds for random mode (default: 10)')
    parser.add_argument('--node', type=str, help='Specific node (service name) to kill')
    
    args = parser.parse_args()

    if args.node:
        kill_container(args.node)
    elif args.random:
        try:
            run_random_chaos(args.interval)
        except KeyboardInterrupt:
            print("\n😇 Chaos Monkey stopped.")
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
