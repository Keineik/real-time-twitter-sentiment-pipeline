kafka=/opt/bitnami/kafka/bin

up:
	docker compose -f ./docker/docker-compose.yaml up --detach
down:
	docker compose -f ./docker/docker-compose.yaml down --volumes
stop:
	docker compose -f ./docker/docker-compose.yaml stop
start:
	docker compose -f ./docker/docker-compose.yaml start
setup-stream:
	- docker exec -u 0 kafka sh -c "$(kafka)/kafka-topics.sh --create --topic twitter-kafka-stream --bootstrap-server localhost:9092;";
	docker exec -u 0 kafka sh -c "mkdir -p /opt/tmp/data"
	docker cp ./src/main/resources/data/small_subset.json kafka:/opt/tmp/data;
start-stream:
	docker exec -u 0 kafka sh -c "$(kafka)/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic twitter-kafka-stream < /opt/tmp/data/small_subset.json"
