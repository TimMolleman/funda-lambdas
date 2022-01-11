import os

BOT_NAME = 'link_scraper'

SPIDER_MODULES = ['link_scraper.scraper.spiders']
NEWSPIDER_MODULE = 'link_scraper.scraper.spiders'

# Obey robots.txt rules
ROBOTSTXT_OBEY = True

# AWS stuff
AWS_ACCESS_KEY_ID = os.environ['AMAZON_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AMAZON_SECRET_ACCESS_KEY']

ITEM_PIPELINE = {
    'fundascraper.pipelines.files.S3FilesStore': 1
}

HTTPCACHE_ENABLED = True
HTTPCACHE_EXPIRATION_SECS = 60 * 60 * 24 * 7
HTTPCACHE_DIR = 'httpcache'
CLOSESPIDER_PAGECOUNT = 40
