# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import json
import os

import boto3 as boto3
from itemadapter import ItemAdapter
import pandas as pd
import datetime as dt


def create_nutrition_df(data, name):
    transformed_data = [name] + [row[2] for row in data["data"]]
    columns = ["id"] + [row[0] for row in data["data"]]
    return pd.DataFrame([transformed_data], columns=columns)


def create_df(data: dict):
    return pd.DataFrame([data.values()], columns=list(data.keys()))


class AuchanPipeline:

    dataframes = {}
    item_counts = {}
    as_of_date = ''
    base_path = ''
    item_count = 0
    part_file = 0
    SAVE_CONSOLIDATED = False

    def open_spider(self, spider):
        self.base_path = spider.settings.get('BASE_PATH')
        self.as_of_date = dt.datetime.now().strftime('%Y_%m_%d')

    def add_to_df_list(self, detail, df):
        if detail in self.dataframes:
            self.dataframes[detail].append(df)
        else:
            self.dataframes[detail] = [df]

    def create_product_df(self, data: dict):
        price_data = data["selectedVariant"]["price"]
        price_data["id"] = data["selectedVariant"]["id"]
        self.add_to_df_list("prices", create_df(price_data))

        del data["selectedVariant"]["price"]
        del data["selectedVariant"]["packageInfo"]["unitPrice"]
        data["selectedVariant"]["packageUnit"] = data["selectedVariant"]["packageInfo"]["packageUnit"]
        data["selectedVariant"]["packageSize"] = data["selectedVariant"]["packageInfo"]["packageSize"]
        del data["selectedVariant"]["packageInfo"]

        self.add_to_df_list("variants", create_df(data["selectedVariant"]))

        data["selectedVariant"] = data["selectedVariant"]["id"]
        data["defaultVariant"] = data["defaultVariant"]["id"]

        self.add_to_df_list("products", create_df(data))

    def process_item(self, item, spider):
        for detail in item:
            if detail not in self.item_counts:
                self.item_counts[detail] = 1
            else:
                self.item_counts[detail] += 1

            if detail == 'nutrition':
                df = create_nutrition_df(item[detail]['data'], item[detail]['variantId'])
                self.add_to_df_list('nutrition', df)
            elif detail == 'product':
                self.create_product_df(item[detail]['data'])
            else:
                df = pd.DataFrame({
                    'itemId': [item[detail]['itemId']],
                    'variantId': [item[detail]['variantId']],
                    'data': json.dumps(item[detail]["data"])
                })
                self.add_to_df_list(detail, df)

            if self.item_counts[detail] % 1000 == 0:
                self.save_part(spider)

        return item

    def save_part(self, spider):
        for detail in self.dataframes:
            df = pd.concat(self.dataframes[detail], ignore_index=True)
            interim_path = f'csv/{self.as_of_date}/{detail}'
            path = f'{self.base_path}/databases/{interim_path}'

            if not os.path.exists(path):
                os.makedirs(path)

            # last_part = int(sorted(os.listdir(path))[-1].split(".")[1]) + 1
            part_filename = f'{detail}.{self.part_file:03}.csv.xz'

            filename = f'{path}/{part_filename}'
            print(f'Saving {filename}...')
            df.to_csv(filename, index=False, compression='xz')

            self.upload_to_s3(
                spider,
                filename,
                f'{interim_path}/{part_filename}'
            )

        self.part_file += 1
        self.dataframes = {}

    def close_spider(self, spider):
        if self.SAVE_CONSOLIDATED:
            interim_path = f'csv/{self.as_of_date}'
            path = f'{self.base_path}/databases/{interim_path}'

            if not os.path.exists(path):
                os.makedirs(path)

            for detail in os.listdir(path):
                for file in os.listdir(f'{path}/{detail}'):
                    df = pd.read_csv(f'{path}/{detail}/{file}')
                    self.add_to_df_list(detail, df)
                    os.remove(f'{path}/{detail}/{file}')

                df = pd.concat(self.dataframes[detail], ignore_index=True)
                filename = f'{path}/{detail}.csv.xz'

                df.to_csv(filename, index=False, compression='xz')

                self.upload_to_s3(
                    spider,
                    filename,
                    f'{interim_path}/{detail}.csv.xz'
                )
        else:
            self.save_part(spider)

    def upload_to_s3(self, spider, filename, object_name):
        # upload to digitalocean s3 compatible storage using boto3
        access_key = spider.ACCESS_KEY
        secret_key = spider.SECRET_KEY

        s3 = boto3.resource('s3',
                            endpoint_url='https://auchan-raw-data.fra1.digitaloceanspaces.com',
                            aws_access_key_id=access_key,
                            aws_secret_access_key=secret_key)

        bucket_name = 'auchan-raw-data'
        bucket = s3.Bucket(bucket_name)
        response = bucket.upload_file(filename, object_name)
        print(response)  # Prints None


