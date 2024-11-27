# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class LevisItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    store_no = scrapy.Field()
    name = scrapy.Field()
    latitude = scrapy.Field()
    longitude = scrapy.Field()
    street = scrapy.Field()
    city = scrapy.Field()
    state = scrapy.Field()
    zip_code = scrapy.Field()
    county = scrapy.Field()
    phone = scrapy.Field()
    open_hours = scrapy.Field()
    url = scrapy.Field()
    provider = scrapy.Field()
    category = scrapy.Field()
    updated_date = scrapy.Field()
    country = scrapy.Field()
    status = scrapy.Field()
    direction_url = scrapy.Field()
    pagesave_path = scrapy.Field()

