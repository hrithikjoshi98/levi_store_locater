import hashlib
from urllib.parse import urlparse
import parsel
import scrapy
import json
from scrapy.cmdline import execute
from levis.items import LevisItem
from levis.db_config import config
import pymysql
import datetime
import os
import gzip
from parsel import Selector
import re


def get_store_no(text):
    match = re.search(r'(\d*).html', text)
    if match:
        return match.group(1)
    else:
        return None

def remove_extra_space(row_data):
    # Remove any extra spaces or newlines created by this replacement
    value = re.sub(r'\s+', ' ', row_data).strip()
    # Update the cleaned value back in row_data
    return value


def format_schedule(schedule):
    # Mapping of abbreviated day names to full names
    day_map = {
        "Su": "Sunday",
        "Mo": "Monday",
        "Tu": "Tuesday",
        "We": "Wednesday",
        "Th": "Thursday",
        "Fr": "Friday",
        "Sa": "Saturday"
    }

    # Split the input string into individual parts
    parts = schedule.split()

    # Iterate over the parts and format them
    i = 0
    stri = ''
    while i < len(parts):
        day_abbr = parts[i]
        try:
            stri += ' | ' + day_map[day_abbr] + ': '
        except Exception as e:
            if day_abbr != ' ':
                stri += str(day_abbr)
        i += 1
    formatted_schedule_str = remove_extra_space(stri).replace('|', '', 1).strip()
    return formatted_schedule_str


def generate_hashid(url: str) -> str:
    # Parse the URL and use the netloc and path as a unique identifier
    parsed_url = urlparse(url)
    unique_string = parsed_url.netloc + parsed_url.path
    # Create a hash of the unique string using SHA-256 and take the first 8 characters
    hash_object = hashlib.sha256(unique_string.encode())
    hashid = hash_object.hexdigest()[:8]  # Take the first 8 characters
    return hashid

class LeviSpider(scrapy.Spider):
    name = "levi"
    start_urls = ["https://locations.levi.com/en-us/"]

    def my_print(self, tu):
        for i in tu:
            print(i)
        print('\n')

    def __init__(self, start_id, end_id, **kwargs):
        super().__init__(**kwargs)
        self.start_id = start_id
        self.end_id = end_id

        self.conn = pymysql.connect(
            host=config.host,
            user=config.user,
            password=config.password,
            db=config.database,
            autocommit=True
        )
        self.cur = self.conn.cursor()

        self.domain = self.start_urls[0].split('://')[1].split('/')[0]
        self.date = datetime.datetime.now().strftime('%d_%m_%Y')

        if 'www' in self.domain:
            self.sql_table_name = self.domain.split('.')[1].replace('-','_') + f'_{self.date}' + '_USA'
        else:
            self.sql_table_name = self.domain.split('.')[0].replace('-','_') + f'_{self.date}' + '_USA'
        self.folder_name = self.domain.replace('.', '_').strip()
        config.file_name = self.folder_name

        self.html_path = 'C:\page_source\\' + self.date + '\\' + self.folder_name + '\\'
        if not os.path.exists(self.html_path):
            os.makedirs(self.html_path)
        # print(self.domain, self.folder_name, self.sql_table_name)
        config.db_table_name = self.sql_table_name

        field_list = []
        value_list = []
        item = ('store_no', 'name', 'latitude', 'longitude', 'street', 'city',
                  'state', 'zip_code', 'county', 'phone', 'open_hours', 'url',
                  'provider', 'category', 'updated_date', 'country', 'status',
                  'direction_url', 'pagesave_path')
        for field in item:
            field_list.append(str(field))
            value_list.append('%s')
        config.fields = ','.join(field_list)
        config.values = ", ".join(value_list)

        self.cur.execute(f"""CREATE TABLE IF NOT EXISTS {self.sql_table_name}(id int AUTO_INCREMENT PRIMARY KEY,
                                    store_no varchar(100) DEFAULT 'N/A',
                                    name varchar(100) DEFAULT 'N/A',
                                    latitude varchar(100) DEFAULT 'N/A',
                                    longitude varchar(100) DEFAULT 'N/A',
                                    street varchar(500) DEFAULT 'N/A',
                                    city varchar(100) DEFAULT 'N/A',
                                    state varchar(100) DEFAULT 'N/A',
                                    zip_code varchar(100) DEFAULT 'N/A',
                                    county varchar(100) DEFAULT 'N/A',
                                    phone varchar(100) DEFAULT 'N/A',
                                    open_hours varchar(500) DEFAULT 'N/A',
                                    url varchar(500) DEFAULT 'N/A',
                                    provider varchar(100) DEFAULT 'N/A',
                                    category varchar(100) DEFAULT 'N/A',
                                    updated_date varchar(100) DEFAULT 'N/A',
                                    country varchar(100) DEFAULT 'N/A',
                                    status varchar(100) DEFAULT 'N/A',
                                    direction_url varchar(500) DEFAULT 'N/A',
                                    pagesave_path varchar(500) DEFAULT 'N/A'
                                    )""")

    # def start_requests(self):
    #     for site_url in self.start_urls:
    #         yield scrapy.FormRequest(site_url, callback=self.parse)

    def parse(self, response, **kwargs):
        selector = Selector(response.text)
        all_state_urls = selector.xpath('//a[@class="region-list ga-link"]/@href').getall()
        for state_url in all_state_urls:
            yield scrapy.Request(state_url, callback=self.get_city_links)


    def get_city_links(self, response):
        selector = Selector(response.text)
        all_city_urls = selector.xpath('//a[@class="city-list"]/@href').getall()
        for city_url in all_city_urls:
            yield scrapy.Request(city_url, callback=self.get_store_links)
        # print('\n')

    def get_store_links(self, response):
        selector = Selector(response.text)
        # all_store_links = selector.xpath('//div[@class="map-list-item-wrap loaded js-is-visible"]//header//a/@href').getall()
        all_store_links = selector.xpath(f'//a[contains(@href, "{response.url}")]/@href').getall()
        all_store_links = list(set(all_store_links))
        for store_link in all_store_links:
            yield scrapy.Request(store_link, callback=self.store_detail_page)

    def store_detail_page(self, response):
        item = LevisItem()
        selector = Selector(response.text)

        url = response.url
        store_no = get_store_no(url)
        json_data = json.loads(selector.xpath('//script[@type="application/ld+json"]//text()').get())

        try:
            name = json_data[0]['mainEntityOfPage']['breadcrumb']['itemListElement'][4]['item']['name']
        except Exception as e:
            name = ''

        try:
            latitude = json_data[0]['geo']['latitude']
        except Exception as e:
            latitude = ''

        try:
            longitude = json_data[0]['geo']['longitude']
        except Exception as e:
            longitude = ''

        street = ''
        try:
            for i in json_data[0]['address']:
                if '' != i and '@type' != i and 'telephone' != i:
                    street += json_data[0]['address'][i] + ', '
            street = remove_extra_space(street)[:-1]
        except Exception as e:
            street = ''

        try:
            city = json_data[0]['address']['addressLocality']
        except Exception as e:
            city = ''

        try:
            state = json_data[0]['address']['addressRegion']
        except Exception as e:
            state = ''

        try:
            zip_code = json_data[0]['address']['postalCode']
        except Exception as e:
            zip_code = ''

        county = 'N/A'

        try:
            phone = json_data[0]['address']['telephone']
        except Exception as e:
            phone = ''

        try:
            o_h = json_data[0]['openingHours']
            open_hours = format_schedule(o_h)
        except Exception as e:
            open_hours = ''

        provider = 'Levi Strauss'
        category = 'Apparel And Accessory Stores'

        updated_date = datetime.datetime.now().strftime("%d-%m-%Y")
        country = 'USA'
        status = 'Open'

        try:
            direction_url = json_data[0]['hasMap']
        except Exception as e:
            direction_url = ''

        page_id = generate_hashid(response.url)
        pagesave_path = self.html_path + fr'{page_id}' + '.html.gz'

        gzip.open(pagesave_path,"wb").write(response.body)


        item['store_no'] = store_no
        item['name'] = name
        item['latitude'] = latitude
        item['longitude'] = longitude
        item['street'] = street
        item['city'] = city
        item['state'] = state
        item['zip_code'] = zip_code
        item['county'] = county
        item['phone'] = phone
        item['open_hours'] = open_hours
        item['url'] = url
        item['provider'] = provider
        item['category'] = category
        item['updated_date'] = updated_date
        item['country'] = country
        item['status'] = status
        item['direction_url'] = direction_url
        item['pagesave_path'] = pagesave_path
        yield item


if __name__ == '__main__':
    # execute("scrapy crawl kia".split())
    execute(f"scrapy crawl levi -a start_id=0 -a end_id=100 -s CONCURRENT_REQUESTS=6".split())