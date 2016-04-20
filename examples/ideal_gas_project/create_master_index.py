# create_master_index.py
import json
from signac.contrib.crawler import MasterCrawler

master_crawler = MasterCrawler('.')
for doc in master_crawler.crawl(depth=1):
    print(json.dumps(doc))
