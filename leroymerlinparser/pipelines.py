# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import hashlib
import scrapy
from scrapy.pipelines.images import ImagesPipeline
from scrapy.utils.python import to_bytes


class LeroymerlinparserPipeline:
    def process_item(self, item, spider):
        item['specifications'] = self.specif_constraction(item['specifications_name'], item['specifications_values'])
        del item['specifications_name']
        del item['specifications_values']
        return item

    def specif_constraction(self, names, values):
        dst = dict(zip(names, values))
        return dst

class PhotoPipeline(ImagesPipeline):
    def get_media_requests(self, item, info):
        if item['photos']:
            for img in item['photos']:
                try:
                    yield scrapy.Request(img)
                except Exception as e:
                    print(e)

    def item_completed(self, results, item, info):
        item['photos'] = [itm[1] for itm in results if itm[0]]
        return item

    def file_path(self, request, item, response=None, info=None):
        image_guid = hashlib.sha1(to_bytes(item['link'])).hexdigest()
        name = item['name'].replace('/', '-')
        return f'{name}/{image_guid}.jpg'
