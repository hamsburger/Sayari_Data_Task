from pipelines import SayariGraphScrapingPipeline
import json
import os

root_path = os.path.dirname(os.path.dirname(__file__))
output_dir = os.path.join(root_path, "output")

pipeline = SayariGraphScrapingPipeline()
pipeline.open_spider(None)

with open(os.path.join(output_dir, "company_records.jsonl")) as f:
    for line in f:
        item = json.loads(line)
        pipeline.process_item(item, None)

pipeline.close_spider(None)
