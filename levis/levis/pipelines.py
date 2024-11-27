# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from levis.db_config import config

class LevisPipeline:
    def __init__(self):
        self.cf = config()

    def process_item(self, item, spider):
        print(self.cf.db_table_name)
        insert_db = f"INSERT IGNORE INTO {self.cf.db_table_name}( " + self.cf.fields + " ) values ( " + self.cf.values + " )"

        spider.cur.execute(insert_db, tuple(item.values()))
        # print('Data Inserted...')
        return item
