import scrapy


class HhruSpider(scrapy.Spider):
    name = 'hhru'
    allowed_domains = ['hh.ru']
    start_urls = ['http://hh.ru/']

    def parse(self, response):
        pass
