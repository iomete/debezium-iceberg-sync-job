image 	:= iomete.azurecr.io/iomete/debezium-iceberg-sync-job:1.2.0

run:
	./gradlew quarkusDev

.PHONY: build
build:
	./gradlew clean quarkusBuild

.PHONY: docker-build
docker-build: build
	docker build -f Dockerfile -t $(image) .
	@echo $(image)

.PHONY: docker-push
docker-push: build
	docker buildx build --platform linux/amd64,linux/arm64 -sbom=true --provenance=true -f Dockerfile -t $(image) --push .
	@echo $(image)

docker-debug:
	docker run -it $(image) bash

docker-run:
	docker run -it --rm \
		-v ${PWD}/local-docker-run/conf:/opt/spark/conf \
		-v ${PWD}/local-docker-run/lakehouse:/lakehouse \
		-v ${PWD}/application.properties:/etc/application.properties \
		-e SPARK_LOCAL_IP=127.0.0.1 \
		-e QUARKUS_PROFILE=prod \
		$(image) driver --class io.debezium.server.Main spark-internal
