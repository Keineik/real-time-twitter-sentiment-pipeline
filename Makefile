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
produce-stream:
	@echo "Producing stream with 100ms delay between lines..."
	@docker exec -u 0 kafka sh -c ' \
		input_file="/opt/tmp/data/small_subset.json"; \
		topic="twitter-kafka-stream"; \
		broker="localhost:9092"; \
		kafka_producer="$(kafka)/kafka-console-producer.sh"; \
		\
		while IFS= read -r line || [ -n "$$line" ]; do \
			printf "%s\n" "$$line" | "$$kafka_producer" --bootstrap-server "$$broker" --topic "$$topic"; \
			sleep 0.1; \
		done < "$$input_file" \
	'
	@echo "Finished producing stream."
consume-stream:
	docker exec -u 0 kafka sh -c "$(kafka)/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic twitter-kafka-stream --from-beginning;"