# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class FundascraperItem(scrapy.Item):
    # define the fields for your item here like:
    city = scrapy.Field()
    link = scrapy.Field()
    rooms = scrapy.Field()
    house_surface = scrapy.Field()
    garden_surface = scrapy.Field()
    price = scrapy.Field()
