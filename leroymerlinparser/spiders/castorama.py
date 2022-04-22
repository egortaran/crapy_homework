import scrapy
from scrapy.http import HtmlResponse
from scrapy.loader import ItemLoader
from leroymerlinparser.items import LeroyparserItem


class CastoramaSpider(scrapy.Spider):
    name = 'castorama'
    allowed_domains = ['castorama.ru']
    start_urls = ['https://www.castorama.ru/lighting/interior-lighting/chandeliers-and-pendants']

    def parse(self, response: HtmlResponse):
        next_page = response.xpath("//a[contains(@class, 'next i-next')]")
        if next_page:
            yield response.follow(next_page[1], callback=self.parse)
        links = response.xpath("//a[@class='product-card__img-link']")
        for link in links:
            yield response.follow(link, callback=self.item_parse)

    def item_parse(self, response: HtmlResponse):
        loader = ItemLoader(item=LeroyparserItem(), response=response)
        loader.add_xpath('name', '//h1/text()')
        loader.add_xpath('price', "//span[@class='regular-price']/span/span/span/text()")
        loader.add_value('link', response.url)
        loader.add_xpath('photos', '//div[@class="js-zoom-container"]/img/@data-src')
        loader.add_xpath('specifications_name',
                         "//div[@id='specifications']//span[contains(@class,'specs-table__attribute-name')]/text()")
        loader.add_xpath('specifications_values',
                         "//div[@id='specifications']//dd[contains(@class, 'specs-table__attribute-value')]/text()")
        yield loader.load_item()
        print()
