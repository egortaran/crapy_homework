# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import re
import unicodedata
from datetime import datetime

from itemadapter import ItemAdapter
from pymongo import MongoClient


class JobparserPipeline:
    def __init__(self):
        client = MongoClient('127.0.0.1', 27017)
        db_name = f'vacancy_{datetime.now().date()}'
        self.mongo_base = client.get_database(db_name)

    def process_item(self, item, spider):
        if spider.name == 'hhru':
            item['salary_min'], item['salary_max'], item['currency'] = self.parse_salary_hh(item['salary'])
        else:
            item['salary_min'], item['salary_max'], item['currency'] = self.parse_salary_sj(item['salary'])

        del (item['salary'])

        collection = self.mongo_base[spider.name]
        collection.insert_one(item)
        return item

    def parse_salary_hh(self, salary):
        min_salary = None
        max_salary = None
        salary_currency = None
        salary = self.validate_data(salary)

        if salary[0] == 'от' and salary[2] == 'до':
            min_salary = int(salary[1])
            max_salary = int(salary[3])
            salary_currency = salary[5]
        elif salary[0] == 'до':
            min_salary = None
            max_salary = int(salary[1])
            salary_currency = salary[3]
        elif salary[0] == 'от':
            min_salary = int(salary[1])
            max_salary = None
            salary_currency = salary[3]
        return min_salary, max_salary, salary_currency

    def parse_salary_sj(self, salary):
        min_salary = None
        max_salary = None
        salary_currency = None
        salary = self.validate_data_sj(salary)
        if salary:
            if '—' in salary:
                salary_min = salary.split('—')[0]
                min_salary = int(re.sub(r'[^0-9]', '', salary_min))
                salary_max = salary.split('—')[1]
                max_salary = int(re.sub(r'[^0-9]', '', salary_max))
                salary_currency = re.search(r'\D+$', salary).group().strip()
            elif 'от' in salary:
                salary_min = salary[2:]
                min_salary = int(re.sub(r'[^0-9]', '', salary_min))
                max_salary = None
                salary_currency = re.search(r'\D+$', salary_min).group().strip()
            elif 'до' in salary:
                min_salary = None
                salary_max = salary[2:]
                max_salary = int(re.sub(r'[^0-9]', '', salary_max))
                salary_currency = re.search(r'\D+$', salary_max).group().strip()
            else:
                min_salary = int(re.sub(r'[^0-9]', '', salary))
                max_salary = int(re.sub(r'[^0-9]', '', salary))
                salary_currency = re.search(r'\D+$', salary).group().strip()

        return min_salary, max_salary, salary_currency

    def validate_data(self, data):
        if data:
            for num, elem in enumerate(data):
                elem = unicodedata.normalize("NFKD", elem)
                elem = elem.strip().replace(' ', '')
                data[num] = elem
        else:
            data = None
        return data

    def validate_data_sj(self, data):
        if data:
            data = ' '.join(data[:-2])
            data = unicodedata.normalize("NFKD", data)
        else:
            data = None
        return data
