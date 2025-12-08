import argparse
import subprocess
import time
import random
import sys
import threading

EXCLUDED_CONTAINERS = ['rabbitmq', 'gateway', 
                    #   'filter-year-1', 
                    #   'filter-year-2', 
                    #   'filter-year-eof-service',
                       'client_1', 'client_2', 
                    #   'joiner-menu-items', 
                    #   'aggregator-month-1', 
                    #   'aggregator-month-2', 
                    #   'aggregator-month-eof-service',
                    #   'join-stores-q3',
                    #   'join-stores-q4',
                    #   'aggregator-store-q3-1', 'aggregator-store-q3-2', 'aggregator-store-q3-eof-service',
                    #   'aggregator-store-q4-1', 'aggregator-store-q4-2', '',
                    #   'filter-time-1', 'filter-time-2', 'filter-time-eof-service',
                       'top-three-clients-1', 'top-three-clients-2', 'top-three-clients-3',
                       #'joiner-stores-q4',
                    #   'aggregator-store-q4', 
                        'health-monitor-1', 'health-monitor-2', 'health-monitor-3'
                        ]

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

def run_top_three_chaos(interval):
    """Kills all top-three containers every interval seconds."""
    print(f"😈 Starting Chaos Monkey (Top-Three Mode). Interval: {interval}s")
    print(f"🎯 Target services: top-three-clients-1, top-three-clients-2, top-three-clients-3")
    
    while True:
        containers = get_running_containers()
        # Filter for top-three containers
        targets = [c for c in containers if c in ['top-three-clients-1', 'top-three-clients-2', 'top-three-clients-3']]
        
        if not targets:
            print("No top-three containers found.")
        else:
            print(f"Found {len(targets)} top-three container(s): {targets}")
            for target in targets:
                kill_container(target)
        
        print(f"Sleeping for {interval} seconds...")
        time.sleep(interval)

def run_monitors_chaos(interval):
    """Kills all health-monitor containers every interval seconds."""
    print(f"😈 Starting Chaos Monkey (Health Monitors Mode). Interval: {interval}s")
    print(f"🎯 Target services: health-monitor-1, health-monitor-2, health-monitor-3")
    
    while True:
        containers = get_running_containers()
        # Filter for health-monitor containers
        targets = [c for c in containers if c in ['health-monitor-1', 'health-monitor-2', 'health-monitor-3']]
        
        if not targets:
            print("No health-monitor containers found.")
        else:
            print(f"Found {len(targets)} health-monitor container(s): {targets}")
            # Kill one random monitor to test recovery
            target = random.choice(targets)
            kill_container(target)
        
        print(f"Sleeping for {interval} seconds...")
        time.sleep(interval)
        
def run_query_chaos(interval, possible_targets):
    while True:
        containers = get_running_containers()

        # Intersect running containers ∩ targets
        targets = [c for c in containers if c in possible_targets]

        if not targets:
            print("⚠️ No target containers found.")
        else:
            # Elegir hasta 5 contenedores al azar
            kill_count = min(6, len(targets))
            chosen = random.sample(targets, kill_count)

            print(f"🔥 Selected {kill_count} container(s) to KILL: {chosen}")

            for target in chosen:
                kill_container(target)

        print(f"⏳ Sleeping for {interval}s...\n")
        time.sleep(interval)

def run_q1_chaos(interval):
    """Kills 5 random target containers every `interval` seconds."""
    print(f"😈 Starting Chaos Monkey (Q1 Mode). Interval: {interval}s")
    print(f"🎯 Target services: 5 random from filter/time/year family")

    possible_targets = [
        'filter-amount-1', 'filter-amount-2', 'filter-amount-3',
        'filter-time-1', 'filter-time-2', 'filter-time-eof-service',
        'filter-year-1', 'filter-year-2', 'filter-year-eof-service'
    ]

    run_query_chaos(interval, possible_targets)
    
        
def run_q2_chaos(interval):
    """Kills 5 random target containers every `interval` seconds."""
    print(f"😈 Starting Chaos Monkey (Q1 Mode). Interval: {interval}s")
    print(f"🎯 Target services: 5 random from filter/time/year family")

    possible_targets = [
        'aggregator-month-1', 'aggregator-month-2',
        'aggregator-quantity-profit-1', 'aggregator-quantity-profit-2', 'aggregator-quantity-profit-3',
        'joiner-menu-items'
    ]
    
    run_query_chaos(interval, possible_targets)
    
def run_q3_chaos(interval):
    """Kills 5 random target containers every `interval` seconds."""
    print(f"😈 Starting Chaos Monkey (Q3 Mode). Interval: {interval}s")
    print(f"🎯 Target services: 5 random from joiner/stores/q3 and aggregator/store/q3 family")

    possible_targets = [
        'join-stores-q3',
        'aggregator-store-q3-1', 'aggregator-store-q3-2',
        'aggregator-semester-1', 'aggregator-semester-1',
    ]
    
    run_query_chaos(interval, possible_targets)
    
def run_q4_chaos(interval):
    """Kills 5 random target containers every `interval` seconds."""
    print(f"😈 Starting Chaos Monkey (Q3 Mode). Interval: {interval}s")
    print(f"🎯 Target services: 5 random from joiner/stores/q3 and aggregator/store/q3 family")

    possible_targets = [
        'join-stores-q3',
        'aggregator-store-stores_state-1', 'aggregator-store-stores_state-2', "aggregator-store-stores_state-3"
        'aggregator-semester-1', 'aggregator-semester-1',
    ]
    
    run_query_chaos(interval, possible_targets)


def run_combined_chaos(random_interval, top_three_interval):
    """Runs both random and top-three chaos simultaneously in separate threads."""
    print(f"😈 Starting Chaos Monkey (Combined Mode)")
    print(f"   - Random Mode Interval: {random_interval}s")
    print(f"   - Top-Three Mode Interval: {top_three_interval}s")
    print(f"🛡️  Excluded services: {EXCLUDED_CONTAINERS}")
    
    # Thread para modo random
    random_thread = threading.Thread(target=run_random_chaos, args=(random_interval,), daemon=True)
    # Thread para modo top-three
    top_three_thread = threading.Thread(target=run_top_three_chaos, args=(top_three_interval,), daemon=True)
    
    random_thread.start()
    top_three_thread.start()
    
    # Keep main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n😇 Chaos Monkey stopped.")


def main():
    parser = argparse.ArgumentParser(description='Chaos Monkey for Docker Compose')
    
    # Modos generales
    parser.add_argument('--random', action='store_true',
                        help='Enable random killing mode')
    parser.add_argument('--top-three', action='store_true',
                        help='Enable top-three killing mode')
    parser.add_argument('--combined', action='store_true',
                        help='Enable both random and top-three modes simultaneously')
    
    # Intervalos
    parser.add_argument('--interval', type=float, default=0.2,
                        help='Interval in seconds for random mode (default: 1)')
    parser.add_argument('--top-three-interval', type=float, default=15,
                        help='Interval in seconds for top-three mode (default: 15)')
    
    # Kill de un nodo puntual
    parser.add_argument('--node', type=str,
                        help='Specific node (service name) to kill once and exit')
    
    # Modos Q1 / Q2 (ARREGLADOS)
    parser.add_argument('--q1', action='store_true',
                        help='Enable q1 killing mode')
    parser.add_argument('--q1-interval', type=float, default=5,
                        help='Interval in seconds for q1 mode (default: 5)')
    
    parser.add_argument('--q2', action='store_true',
                        help='Enable q2 killing mode')
    parser.add_argument('--q2-interval', type=float, default=5,
                        help='Interval in seconds for q2 mode (default: 5)')
    
    
    parser.add_argument('--q3', action='store_true',
                        help='Enable q3 killing mode')
    parser.add_argument('--q3-interval', type=float, default=5,
                        help='Interval in seconds for q3 mode (default: 5)')
    
    parser.add_argument('--monitors', action='store_true',
                        help='Enable health-monitors killing mode')
    parser.add_argument('--monitors-interval', type=float, default=5,
                        help='Interval in seconds for monitors mode (default: 5)')
    
    
    args = parser.parse_args()

    if args.node:
        kill_container(args.node)

    elif args.combined:
        try:
            run_combined_chaos(args.interval, args.top_three_interval)
        except KeyboardInterrupt:
            print("\n😇 Chaos Monkey stopped.")

    elif args.top_three:
        try:
            run_top_three_chaos(args.top_three_interval)
        except KeyboardInterrupt:
            print("\n😇 Chaos Monkey stopped.")

    elif args.random:
        try:
            run_random_chaos(args.interval)
        except KeyboardInterrupt:
            print("\n😇 Chaos Monkey stopped.")

    elif args.q1:
        try:
            run_q1_chaos(args.q1_interval)
        except KeyboardInterrupt:
            print("\n😇 Chaos Monkey stopped.")

    elif args.q2:
        try:
            run_q2_chaos(args.q2_interval)
        except KeyboardInterrupt:
            print("\n😇 Chaos Monkey stopped.")
            
    elif args.q3:
        try:
            run_q3_chaos(args.q3_interval)
        except KeyboardInterrupt:
            print("\n😇 Chaos Monkey stopped.")
    
    elif args.monitors:
        try:
            run_monitors_chaos(args.monitors_interval)
        except KeyboardInterrupt:
            print("\n😇 Chaos Monkey stopped.")

    else:
        parser.print_help()

if __name__ == "__main__":
    main()