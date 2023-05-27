import json
import time

import scrapy


class AuchanLiteSpider(scrapy.Spider):
    name = 'auchan_lite'
    start_urls = ['https://online.auchan.hu/']

    PRODUCTS_PER_PAGE = 1000
    BASE_URL = 'https://online.auchan.hu/api/v2'

    FIELDS_TO_GET = None

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

    authorization = ''

    def start_requests(self):
        self.FIELDS_TO_GET = self.FIELDS_TO_GET.split(',') if self.FIELDS_TO_GET else []
        yield scrapy.Request(
            url='https://online.auchan.hu/',
        )

    def _request(self, url, callback=None, **kwargs):
        if callback is None:
            callback = self.parse
        request = scrapy.Request(
            url=url,
            callback=callback)
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

            yield from self.get_details(product, itemId, variantId)
            yield from self.get_product_fields(itemId, variantId)

            yield {
                'product': {
                    'itemId': itemId,
                    'variantId': variantId,
                    'data': product
                }
            }

        yield from self.get_next_page(response)

    def get_next_page(self, response):
        json_response = json.loads(response.text)
        if json_response['currentPage'] < json_response['pageCount']:
            category = response.url.split('categoryId=')[1].split('&')[0]
            next_page = json_response['currentPage'] + 1
            next_url = f'{self.BASE_URL}/products?page={next_page}&itemsPerPage={self.PRODUCTS_PER_PAGE}&categoryId={category}&hl=hu'
            yield self._request(url=next_url, callback=self.parse_products)

    def get_product_fields(self, itemId, variantId):
        for field in self.PRODUCT_FIELDS:
            if self.FIELDS_TO_GET is not None and field in self.FIELDS_TO_GET:
                yield self._request(
                        url=f'{self.BASE_URL}/products/{itemId}/variants/{variantId}/{field}?hl=hu',
                        callback=self.parse_product_fields)

    def get_details(self, product, itemId, variantId):
        details = product['selectedVariant']['details']
        for field in details:
            if self.FIELDS_TO_GET is not None and field in self.FIELDS_TO_GET:
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
                self.authorization = f'Bearer {access_token}'
                print(self.authorization)
                break

        # get categories list by root category
        yield self._request(url=f'{self.BASE_URL}/tree/0?hl=hu', callback=self.parse_categories)
