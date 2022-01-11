import scrapy
from typing import List
import logging

from link_scraper.scraper import util
from link_scraper.scraper.items import FundascraperItem


class FundaSpider(scrapy.Spider):
    """Spider for crawling most recent day of house listings on Funda.
    """
    name = 'funda_spider'
    start_urls = ['https://www.funda.nl/koop/heel-nederland/1-dag/']
    funda_base_url = 'https://funda.nl'
    links: List[str] = []
    logger = logging.getLogger(__name__)

    def parse(self, response):
        # Get all links to houses on page
        for search_result in response.css('li.search-result'):
            link = search_result.css('.search-result__header-title-col a::attr(href)').get()

            # Only add house listing if it is not already in the links (there are two hrefs with same link per listing)
            if link not in self.links:
                try:
                    # Get the price
                    price = search_result.css('span.search-result-price ::text').get()
                    price = util.extract_digits_from_str(price)

                    # Get the number of rooms and the surface of the house
                    surface = search_result.css('div .search-result-kenmerken span ::text').extract()
                    rooms = search_result.css('div .search-result-kenmerken li ::text').extract()[-1]

                    # Get the city of the house
                    city = search_result.css('h4.search-result__header-subtitle ::text').get()
                    city = city.split()
                    city = (' '.join(city[2:])).lower()

                    # Get house and garden surfaces
                    house_surface = util.extract_digits_from_str(surface[0], square_meters=True)
                    rooms = util.extract_digits_from_str(rooms)
                    garden_surface = 0
                    if len(surface) > 1:
                        garden_surface = util.extract_digits_from_str(surface[1], square_meters=True)

                    # Add FundascraperItem
                    item = FundascraperItem()
                    item['city'] = city
                    item['link'] = f'{self.funda_base_url}{link}'
                    item['rooms'] = rooms
                    item['house_surface'] = house_surface
                    item['garden_surface'] = garden_surface
                    item['price'] = price
                    yield item

                except Exception as e:
                    self.logger.info(f'Skipped {link} because it could not be scraped. Traceback: {e}')

                finally:
                    self.links.append(link)

        next_page_endpoint = response.css('a[rel="next"]::attr(href)').get()

        # Go to next page if possible
        if next_page_endpoint is not None:
            next_page = self.funda_base_url + next_page_endpoint
            yield response.follow(next_page, callback=self.parse)
