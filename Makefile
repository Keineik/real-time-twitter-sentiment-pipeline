up:
	docker compose -f ./docker/docker-compose.yaml up --detach
down:
	docker compose -f ./docker/docker-compose.yaml down --volumes
stop:
	docker compose -f ./docker/docker-compose.yaml stop
start:
	docker compose -f ./docker/docker-compose.yaml start