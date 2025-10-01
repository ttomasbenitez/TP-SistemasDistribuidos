IMAGE_NAME := test-runner

build:
	docker build -t $(IMAGE_NAME) -f Dockerfile ..

test: build
	docker run --rm -v "$$PWD/..":/app -w /app $(IMAGE_NAME) pytest -s test

shell: build
	docker run --rm -it -v "$$PWD/..":/app -w /app $(IMAGE_NAME) bash

clean:
	docker rmi $(IMAGE_NAME)
.PHONY: build test shell clean
