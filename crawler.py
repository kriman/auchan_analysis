import io
import json
import os
import scrapy
from scrapy.exceptions import DropItem
import tarfile

PREVIOUS_SCRAPE = "databases/archives/2022_12_27_rawdata.tar.xz"
NEW_SCRAPE = "databases/archives/2023_01_08_rawdata_diff.tar"


class AuchanSpider(scrapy.Spider):
    name = 'auchan_spider'
    start_urls = ['https://online.auchan.hu/api/v2/products?page=1&itemsPerPage=12&listId=10177&hl=hu']

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

    """
    'FEEDS': {
            'products2.csv': {
                'format': 'csv',
                'overwrite': True
            }
        },
    """

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

    # authorization = open('authorization.txt', 'r').read()
    authorization = str()

    tar = tarfile.open(PREVIOUS_SCRAPE, "r:xz")

    def start_requests(self):
        self.tar_names = self.tar.getnames()
        yield scrapy.Request(
            url='https://online.auchan.hu/',
        )

    def end_requests(self):
        self.tar.close()

    def _request(self, url):
        request = scrapy.Request(
            url=url,
            callback=self.parse)
        for key, value in self.custom_settings['DEFAULT_REQUEST_HEADERS'].items():
            request.headers[key] = value
        request.headers["Authorization"] = (self.authorization)
        return request

    def parse(self, response):

        def _get_details(details, itemId, variantId, detail_field="detail/"):
            print(f'Parsing details for {itemId}/{variantId}')
            print(details)
            for field in details:
                # if not in tarfile
                print(f'Parsing {field}')
                if f'rawdata/{itemId}/{variantId}/{field}.json' not in self.tar_names:
                    print("not in tarfile")
                    yield self._request(
                        url=f'{self.BASE_URL}/products/{itemId}/variants/{variantId}/{detail_field}{field}?hl=hu')
                else:
                    # yield back from the tarfile
                    print(f'yielding back from tarfile: {f"rawdata/{itemId}/{variantId}/{field}.json"}')
                    yield {
                        field: {
                            'itemId': itemId,
                            'variantId': variantId,
                            'data': json.loads(
                                self.tar.extractfile(f'rawdata/{itemId}/{variantId}/{field}.json').read())
                        }
                    }
            return True

        # parse json response
        if response.url == 'https://online.auchan.hu/':
            cookies = response.headers.getlist('Set-Cookie')
            print(cookies)
            for cookie in cookies:
                if cookie.startswith(b'access_token'):
                    access_token = cookie.split(b'=')[1].split(b';')[0].decode('utf-8')
                    self.custom_settings['DEFAULT_REQUEST_HEADERS']['Authorization'] = f'Bearer {access_token}'
                    self.authorization = f'Bearer {access_token}'
                    print(self.authorization)
                    break

            for category in range(15000, 5500, -1):
                yield self._request(
                    url=f'{self.BASE_URL}/products?page=1&itemsPerPage={self.PRODUCTS_PER_PAGE}&categoryId={category}&hl=hu')

        else:
            data = json.loads(response.text)
            if 'results' in data:
                print(f'Parsing category: {data["results"][0]["categoryId"]}')
                for product in data['results']:
                    itemId = product['id']
                    variantId = product['selectedVariant']['id']
                    # if not os.path.exists(f'rawdata/{itemId}/{variantId}/product.json'):
                    yield {
                        'product': {
                            'itemId': itemId,
                            'variantId': variantId,
                            'data': product
                        }
                    }

                    # get details
                    details = product['selectedVariant']['details']
                    for field in details:
                        # if not in tarfile
                        if f'rawdata/{itemId}/{variantId}/{field}.json' not in self.tar_names:
                            yield self._request(
                                url=f'{self.BASE_URL}/products/{itemId}/variants/{variantId}/details/{field}?hl=hu')

                    for field in self.PRODUCT_FIELDS:
                        # if not in tarfile
                        if f'rawdata/{itemId}/{variantId}/{field}.json' not in self.tar_names:
                            yield self._request(
                                url=f'{self.BASE_URL}/products/{itemId}/variants/{variantId}/{field}?hl=hu')

                # go to next product page
                if response.json()['currentPage'] < response.json()['pageCount']:
                    category = response.url.split('categoryId=')[1].split('&')[0]
                    next_page = response.json()['currentPage'] + 1
                    next_url = f'{self.BASE_URL}/products?page={next_page}&itemsPerPage={self.PRODUCTS_PER_PAGE}&categoryId={category}&hl=hu'
                    yield self._request(url=next_url)

            elif 'sectionType' in data:
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
            elif type(data) == list:
                itemId = response.url.split('/')[-4]
                variantId = response.url.split('/')[-2]
                yield {
                    'stock_infos': {
                        'itemId': itemId,
                        'variantId': variantId,
                        'data': data
                    }
                }


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


class SaveBaseInfo(object):
    def process_item(self, item, spider):
        """text processing"""
        if 'product' in item:
            item = item['product']
            return {
                'id': item['id'],
                'defaultVariant': item['defaultVariant']['id'],
                'productId': item['defaultVariant']['productId'],
                'price': item['defaultVariant']['price']['gross'],
                'name': item['defaultVariant']['name'],
                'categoryId': item['categoryId'],
            }
        else:
            raise DropItem('not product')


from scrapy.crawler import CrawlerProcess

process = CrawlerProcess()
res = process.crawl(AuchanSpider)
process.start()
