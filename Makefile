SPIDER_NAME=business_spider
IMAGE_NAME=$(SPIDER_NAME)_image
PROJECT_NAME=sayari_graph_scraping
DATA_FILE=$(PROJECT_NAME)/output/company_records.jsonl

ALL: build crawl

# Build docker image
build:
	docker build -t $(IMAGE_NAME) .

run:
	docker run --rm -v $$PWD:/app $(IMAGE_NAME)

crawl:
	docker run --rm -v $$PWD:/app $(IMAGE_NAME) scrapy crawl $(SPIDER_NAME)

postprocess:
	docker run --rm -v $$PWD:/app $(IMAGE_NAME) python $(PROJECT_NAME)/postprocess.py

# Stop container and remove docker image
clean-image:
	CONTAINER_IDS=$$(docker ps -a -q --filter ancestor=$(IMAGE_NAME)); \
	if [ -z "$$CONTAINER_IDS" ]; then \
		echo "No containers found for image: $(IMAGE_NAME)"; \
	else \
		echo "Stopping containers..."; \
		docker stop $$CONTAINER_IDS; \
		echo "Removing containers..."; \
		docker rm $$CONTAINER_IDS; \
	fi; \
	echo "Removing image: $(IMAGE_NAME)"; \
	docker rmi -f $(IMAGE_NAME) || true; \
	echo "Done cleaning up image and containers."