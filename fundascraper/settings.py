import os

BOT_NAME = 'fundascraper'

SPIDER_MODULES = ['fundascraper.spiders']
NEWSPIDER_MODULE = 'fundascraper.spiders'

# Obey robots.txt rules
ROBOTSTXT_OBEY = True

# AWS stuff
AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['C+EZLpGKSp+HCpu/ieZAgBagofTREuC3YV8D4s2h']

ITEM_PIPELINE = {
    'fundascraper.pipelines.files.S3FilesStore': 1
}

HTTPCACHE_ENABLED = True
HTTPCACHE_EXPIRATION_SECS = 60 * 60 * 24 * 7
HTTPCACHE_DIR = 'httpcache'
CLOSESPIDER_PAGECOUNT = 40
