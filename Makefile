docker-image:
	docker build -f ./gateway/Dockerfile -t "gateway:latest" .
	docker build -f ./client/Dockerfile -t "client:latest" .
	docker build -f ./filters/Dockerfile -t "filter:latest" .
	# Execute this command from time to time to clean up intermediate stages generated 
	# during client build (your hard drive will like this :) ). Don't left uncommented if you 
	# want to avoid rebuilding client image every time the docker-compose-up command 
	# is executed, even when client code has not changed
	# docker rmi `docker images --filter label=intermediateStageToBeDeleted=true -q`
.PHONY: docker-image

up: docker-image
	docker compose -f docker-compose-dev.yaml up -d --build
.PHONY: up

run: 
	docker compose -f docker-compose-dev.yaml up --build
.PHONY: run

down:
	docker compose -f docker-compose-dev.yaml stop -t 1
	docker compose -f docker-compose-dev.yaml down
.PHONY: down

logs:
	docker compose -f docker-compose-dev.yaml logs -f
.PHONY: logs
