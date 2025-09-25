import json
import yaml

REPLICATED_QUEUES = ["menu-items-queue", "stores-q3-queue", "stores-q4-queue", "users-queue"]
CONFIG_FILE = "compose_config.json"
COMPOSE_FILE = "docker-compose.yaml"

def load_json(file_path):
    with open(file_path) as f:
        return json.load(f)

def count_queue_consumers(services):
    queue_consumers = {q: 0 for q in REPLICATED_QUEUES}
    for svc in services:
        replicas = svc.get("replicas", 1)
        if "join" in svc["name"]:
            for v in svc.get("input_queues", {}).values():
                if v in REPLICATED_QUEUES:
                    queue_consumers[v] = replicas
    return queue_consumers

def generate_service_env(service, i, queue_consumers):
    name = service["name"]
    env_vars = {}

    # Input queues
    for k, v in service.get("input_queues", {}).items():
        if "join" in name and v in REPLICATED_QUEUES:
            env_vars[k] = f"{v}-{i}"
        else:
            env_vars[k] = v

    # Output queues
    if name == "gateway":
        idx = 1
        for v in service.get("output_queues", {}).values():
            if v in queue_consumers and queue_consumers[v] > 0:
                for j in range(1, queue_consumers[v] + 1):
                    env_vars[f"OUTPUT_QUEUE_{idx}"] = f"{v}-{j}"
                    idx += 1
            else:
                env_vars[f"OUTPUT_QUEUE_{idx}"] = v
                idx += 1
        if "PORT" in service:
            env_vars["PORT"] = service["PORT"]
    else:
        for k, v in service.get("output_queues", {}).items():
            env_vars[k] = v

    # EXCHANGE_NAME si existe
    if "EXCHANGE_NAME" in service:
        env_vars["EXCHANGE_NAME"] = service["EXCHANGE_NAME"]

    if "log_level" in service:
        env_vars["LOG_LEVEL"] = service["log_level"]

    return env_vars

def generate_docker_compose(data):
    docker_compose = {"version": "3", "services": {}}
    queue_consumers = count_queue_consumers(data["services"])

    for service in data["services"]:
        name = service["name"]
        replicas = service.get("replicas", 1)

        for i in range(1, replicas + 1):
            service_name = f"{name}-{i}" if replicas > 1 else name
            env_vars = generate_service_env(service, i, queue_consumers)

            svc_dict = {
                "environment": env_vars,
                "networks": ["testing_network"],
            }

            if "ports" in service:
                svc_dict["ports"] = service["ports"]

            if name != "rabbitmq":
                svc_dict["depends_on"] = ["rabbitmq"]

            docker_compose["services"][service_name] = svc_dict

    return docker_compose

def main():
    data = load_json(CONFIG_FILE)
    docker_compose = generate_docker_compose(data)
    with open(COMPOSE_FILE, "w") as f:
        yaml.dump(docker_compose, f, sort_keys=False)

if __name__ == "__main__":
    main()
