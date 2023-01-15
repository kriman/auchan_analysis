import io
import json
import tarfile
import time

import scrapy
from scrapy.crawler import CrawlerProcess

PREVIOUS_SCRAPE = "databases/archives/2023_01_08_rawdata.tar.xz"
NEW_SCRAPE = "databases/archives/2023_01_15_rawdata_diff.tar"


class AuchanSpider(scrapy.Spider):
    name = 'auchan_spider'
    start_urls = ['https://online.auchan.hu/']

    custom_settings = {
        'ITEM_PIPELINES': {
            '__main__.RawDataPipeline': 1,
        },
        'DOWNLOAD_DELAY': 0.1,
        'LOG_LEVEL': 'INFO',
        # 'LOG_FILE': 'log.txt',
        'CONCURRENT_REQUESTS': 32,
        'DEFAULT_REQUEST_HEADERS': {
            'Accept': 'application/json',
            'Accept-Language': 'hu',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36',
        }
    }

    PRODUCTS_PER_PAGE = 500
    BASE_URL = 'https://online.auchan.hu/api/v2'

    DETAIL_FIELDS = {
        'nutrition': 'nutritions',
        'ingredients': 'description',
        'parameterList': 'parameters',
        'description': 'description',
        'allergens': 'allergenInfo'
    }

    PRODUCT_FIELDS = {
        'stock_infos': None
    }

    authorization = ""

    tar = tarfile.open(PREVIOUS_SCRAPE, "r:xz")

    tar_names = []

    start_time = -1

    def start_requests(self):
        self.tar_names = self.tar.getnames()
        self.start_time = time.time()
        yield scrapy.Request(
            url='https://online.auchan.hu/',
        )

    def end_requests(self):
        self.tar.close()
        print(f"Time elapsed: {time.time() - self.start_time}")

    def _request(self, url, callback=None, **kwargs):
        if callback is None:
            callback = self.parse
        request = scrapy.Request(
            url=url,
            callback=callback)
        for key, value in self.custom_settings['DEFAULT_REQUEST_HEADERS'].items():
            request.headers[key] = value
        request.headers["Authorization"] = self.authorization
        return request

    def parse_tree(self, node, children, leafs_id):
        if node["childCount"] == 0:
            if node["level"] == 3:
                if "icon" in node:
                    del node["icon"]
                leafs_id.append(node["id"])

        else:
            for child in node["children"]:
                self.parse_tree(child, children, leafs_id)

        children.append(node)

    def parse_categories(self, response):
        print("parse_categories")
        tree = json.loads(response.text)
        children = []
        leafs_id = []
        self.parse_tree(tree, children, leafs_id)
        for leaf_id in leafs_id:
            yield self._request(
                url=f'{self.BASE_URL}/products?categoryId={leaf_id}&page=1&itemsPerPage={self.PRODUCTS_PER_PAGE}&hl=hu',
                callback=self.parse_products
            )
        # tree.json is not saved because it can be generated from product files

    def parse_products(self, response):
        data = json.loads(response.text)
        print(f'Parsing category: {response.url.split("categoryId=")[1].split("&")[0]}')
        for product in data['results']:
            itemId = product['id']
            variantId = product['selectedVariant']['id']
            yield {
                'product': {
                    'itemId': itemId,
                    'variantId': variantId,
                    'data': product
                }
            }

            yield from self.get_details(product, itemId, variantId)
            yield from self.get_product_fields(itemId, variantId)

        yield from self.get_next_page(response)

    def get_next_page(self, response):
        if response.json()['currentPage'] < response.json()['pageCount']:
            category = response.url.split('categoryId=')[1].split('&')[0]
            next_page = response.json()['currentPage'] + 1
            next_url = f'{self.BASE_URL}/products?page={next_page}&itemsPerPage={self.PRODUCTS_PER_PAGE}&categoryId={category}&hl=hu'
            yield self._request(url=next_url, callback=self.parse_products)

    def get_product_fields(self, itemId, variantId):
        for field in self.PRODUCT_FIELDS:
            # if not in tarfile
            if f'rawdata/{itemId}/{variantId}/{field}.json' not in self.tar_names:
                yield self._request(
                    url=f'{self.BASE_URL}/products/{itemId}/variants/{variantId}/{field}?hl=hu',
                    callback=self.parse_product_fields)

    def get_details(self, product, itemId, variantId):
        details = product['selectedVariant']['details']
        for field in details:
            if f'rawdata/{itemId}/{variantId}/{field}.json' not in self.tar_names:
                yield self._request(
                    url=f'{self.BASE_URL}/products/{itemId}/variants/{variantId}/details/{field}?hl=hu',
                    callback=self.parse_details)

    def parse_details(self, response):
        data = json.loads(response.text)

        itemId = response.url.split('/')[-5]
        variantId = response.url.split('/')[-3]

        detail = response.url.split('/')[-1].split('?')[0]
        field = self.DETAIL_FIELDS[detail]
        yield {
            detail: {
                'itemId': itemId,
                'variantId': variantId,
                'data': data[field]
            }
        }

    @staticmethod
    def parse_product_fields(response):
        data = json.loads(response.text)

        itemId = response.url.split('/')[-4]
        variantId = response.url.split('/')[-2]
        yield {
            'stock_infos': {
                'itemId': itemId,
                'variantId': variantId,
                'data': data
            }
        }

    def parse(self, response, **kwargs):
        # set authorization
        cookies = response.headers.getlist('Set-Cookie')
        print(cookies)
        for cookie in cookies:
            if cookie.startswith(b'access_token'):
                access_token = cookie.split(b'=')[1].split(b';')[0].decode('utf-8')
                self.custom_settings['DEFAULT_REQUEST_HEADERS']['Authorization'] = f'Bearer {access_token}'
                self.authorization = f'Bearer {access_token}'
                print(self.authorization)
                break

        # get categories list by root category
        yield self._request(url=f'{self.BASE_URL}/tree/0?hl=hu', callback=self.parse_categories)


class RawDataPipeline(object):
    base_path = 'rawdata'

    tar = tarfile.open(NEW_SCRAPE, 'a')

    def add_to_tarfile(self, data, path):
        file_obj = io.BytesIO(json.dumps(data).encode("utf-8"))

        tarinfo = tarfile.TarInfo(path)
        tarinfo.size = len(file_obj.getvalue())
        self.tar.addfile(tarinfo, file_obj)

    def process_item(self, item, spider):
        for detail in item:
            item_id = item[detail]['itemId']
            variant_id = item[detail]['variantId']
            # save to tarfile
            self.add_to_tarfile(item[detail]['data'], f'{self.base_path}/{item_id}/{variant_id}/{detail}.json')
        return item

    def close_spider(self, spider):
        print("Closing spider")
        self.tar.close()

process = CrawlerProcess()
res = process.crawl(AuchanSpider)
process.start()
