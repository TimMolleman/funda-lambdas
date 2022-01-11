# my_sls_scraper/crawl.py
import sys
import types
import os

from scrapy.spiderloader import SpiderLoader
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from link_scraper.scraper import util

# Need to "mock" sqlite for the process to not crash in AWS Lambda / Amazon Linux
sys.modules["sqlite"] = types.ModuleType("sqlite")
sys.modules["sqlite3.dbapi2"] = types.ModuleType("sqlite.dbapi2")


def crawl(settings={}, spider_name: str = 'funda_spider', spider_kwargs={}) -> str:
    """Function that crawls given a given spider_name. Depending on if the function is called in AWS or locally,
    it will return a different feed_uri."""
    settings_file_path = 'link_scraper.scraper.settings'  # The path seen from root, ie. from main.py
    os.environ.setdefault('SCRAPY_SETTINGS_MODULE', settings_file_path)
    project_settings = get_project_settings()
    spider_loader = SpiderLoader(project_settings)
    print(spider_loader.list())

    spider_cls = spider_loader.load(spider_name)

    # Get feed uri and set the cache_dir if applicable
    feed_uri = util.get_feed_uri()

    if util.is_in_aws():
        settings['HTTPCACHE_DIR'] = '/tmp'

    settings['FEED_URI'] = feed_uri
    settings['FEED_FORMAT'] = 'csv'

    process = CrawlerProcess({**project_settings, **settings})
    process.crawl(spider_cls, **spider_kwargs)
    process.start()

    return feed_uri


if __name__ == '__main__':
    crawl()
