from scraper import crawl
import logging
from library import some_func

logging.basicConfig(level=logging.INFO)


def scrape(event) -> str:
    feed_uri = crawl.crawl(**event)
    return feed_uri


def main(event={}, context={}):
    # First scrape, then make sure only new links are kept
    scrape(event)


if __name__ == '__main__':
    main()
