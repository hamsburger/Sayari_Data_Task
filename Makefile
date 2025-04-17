SPIDER_NAME=business_spider
IMAGE_NAME=$(SPIDER_NAME)_image
PROJECT_NAME=sayari_graph_scraping
DATA_FILE=$(PROJECT_NAME)/output/company_records.jsonl

ALL: build crawl

build:
	docker build -t $(IMAGE_NAME) .

run:
	docker run --rm -v $$PWD:/app $(IMAGE_NAME)

crawl:
	docker run --rm -v $$PWD:/app $(IMAGE_NAME) scrapy crawl $(SPIDER_NAME)

postprocess:
	docker run --rm -v $$PWD:/app $(IMAGE_NAME) python $(PROJECT_NAME)/postprocess.py
 
# if data file exists, only postprocess. Otherwise crawl
autoproc:
	@if docker exec $(IMAGE_NAME) test -f $(DATA_FILE); then \
		echo "✔ Found $$(DATA_FILE). Skipping crawl. Running postprocess only."; \
		make postprocess; \
	else \
		echo "⚙ Data file not found. Crawling now..."; \
		make crawl \
	fi