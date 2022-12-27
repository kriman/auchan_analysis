#!/usr/bin/env python
# coding: utf-8

import sqlite3
import json
import time
import os

# Connecting to sqlite
conn = sqlite3.connect('auchan_scrape.sqlite')
cursor = conn.cursor()

def construct_product(data):
    return [
        data["id"],
        data["categoryId"],
        data["categoryName"],
        data["brandName"],
        data["defaultVariant"]["id"],
        data["selectedVariant"]["id"],
        data["eancode"],
        data["reviewable"],
        data["reviewSum"]["sumCount"],
        data["reviewSum"]["average"],
        data["isNewProduct"],
        data["adultsOnly"],
        data["shipmentDays"],
        data["ageConfirmed"],
        data["isNonFood"]
    ]

def insert_product(row_data):
    insert_command = """
    INSERT INTO products VALUES (
        ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
    )
    """
    try:
        cursor.execute(insert_command, row_data)
    except sqlite3.IntegrityError:
        pass


# # Insert variants

def construct_price(variant_id, price_data, time):
    return [
        variant_id,
        price_data["net"],
        price_data["gross"],
        price_data["currency"],
        price_data["decimalPlaces"],
        price_data["netDiscounted"],
        price_data["grossDiscounted"],
        price_data["discountPercentage"],
        price_data["isDiscounted"],
        time
    ]

def construct_variant(variant_data):
    return [
        variant_data["id"],
        variant_data["name"],
        variant_data["sku"],
        variant_data["productId"],
        variant_data["addedName"],
        variant_data["selectValue"],
        variant_data["status"],
        variant_data["unit"],
        variant_data["eanCode"],
        variant_data["aided"],
        variant_data["loose"]["weightPerPiece"],
        variant_data["packageInfo"]["packageUnit"],
        variant_data["packageInfo"]["packageSize"]
    ]

def construct_nutrition(variant_id, nd):
    values = {
        "energia": "calories",
        "zsír": "fat",
        "telített": "saturated_fat",
        "szénhidrát": "carbohydrates",
        "cukrok": "sugar",
        "cukor": "sugar",
        "rost": "fiber",
        "fehérje": "protein",
        "só": "sodium"
    }
    row = {
        "variant_id": variant_id,
        "calories": 0,
        "fat": 0,
        "saturated_fat": 0,
        "carbohydrates": 0,
        "sugar": 0,
        "fiber": 0,
        "protein": 0,
        "sodium": 0
    }

    for field in nd:
        if field[0].lower().find("energia") != -1:
            row["calories"] = float(field[2].split("/")[1].strip().replace(",", "."))
        else:
            for key in values:
                if field[0].strip().split(" ")[0].lower().find(key) != -1:
                    value_string = field[2].strip()
                    if value_string != '':
                        row[values[key]] = float(value_string.replace(",", "."))
                    else:
                        row[values[key]] = 0
    # convert row to list in the same order as the table
    return [row["variant_id"], row["calories"], row["fat"], row["saturated_fat"], row["carbohydrates"], row["sugar"], row["fiber"], row["protein"], row["sodium"]]

def construct_stock_info(variant_id, stock_info, time):
    return [
        variant_id,
        stock_info["stock"],
        stock_info["store"]["id"],
        time
    ]

def construct_store(sd):
    return [
        sd["id"],
        sd["name"],
        sd["address"]["postCode"],
        sd["address"]["city"],
        sd["address"]["streetName"] + sd["address"]["streetType"] + " " + sd["address"]["streetNo"]
    ]

def gather_variants_info(filename, data):
    variant_rows = []
    price_rows = []
    nutrition_rows = []
    stock_rows = []
    store_rows = []
    for variant in ["defaultVariant", "selectedVariant"]:
        variant_data = data[variant]
        row_data = construct_variant(variant_data)

        price_data = variant_data["price"]
        price_row = construct_price(variant_data["id"], price_data, int(time.time()))

        with open(filename+"nutrition.json") as f:
            nutrition_data = json.loads(f.read())
        nutrition_row = construct_nutrition(variant_data["id"], nutrition_data["data"])
        nutrition_rows.append(nutrition_row)

        with open(filename+"stock_infos.json") as f:
            stock_data = json.loads(f.read())
        tmp_stock_rows = []
        for stock in stock_data:
            stock_row = construct_stock_info(variant_data["id"], stock, int(time.time()))
            store_row = construct_store(stock["store"])
            assert len(stock_row) == 4 and len(store_row) == 5
            tmp_stock_rows.append(stock_row)
            store_rows.append(store_row)
        stock_rows.append(tmp_stock_rows)

        assert len(row_data) == 13 and len(price_row) == 10
        variant_rows.append(row_data)
        price_rows.append(price_row)

    return {
        "variants": variant_rows,
        "prices": price_rows,
        "nutritions": nutrition_rows,
        "stocks": stock_rows,
        "stores": store_rows
    }

def insert_variant_data(variant_info):
    variant_rows = variant_info["variants"]
    price_rows = variant_info["prices"]
    nutrition_rows = variant_info["nutritions"]
    stock_rows = variant_info["stocks"]
    store_rows = variant_info["stores"]

    variant_insert_command = """
    INSERT INTO variants VALUES (
        ?,?,?,?,?,?,?,?,?,?,?,?,?
    )
    """
    price_insert_command = """
    INSERT INTO prices VALUES (
        ?,?,?,?,?,?,?,?,?,?
    )
    """
    nutrition_insert_command = """
    INSERT INTO nutritions VALUES (
        ?,?,?,?,?,?,?,?,?
    )
    """
    stock_insert_command = """
    INSERT INTO stock_infos VALUES (
        ?,?,?,?
    )
    """
    store_insert_command = """
    INSERT INTO stores VALUES (
        ?,?,?,?,?
    )
    """
    for i, row in enumerate(variant_rows):
        try:
            cursor.execute(variant_insert_command, row)

            price = price_rows[i]
            cursor.execute(price_insert_command, price)
            nutrition_row = nutrition_rows[i]
            cursor.execute(nutrition_insert_command, nutrition_row)

            for stock in stock_rows[i]:
                cursor.execute(stock_insert_command, stock)

        except sqlite3.IntegrityError:
            print(f'row: {row[0]} already exists')

    for store in store_rows:
        try:
            cursor.execute(store_insert_command, store)
        except sqlite3.IntegrityError:
            pass

def construct_categories(categories_data):
    categories = []
    for category_data in categories_data:
        category_row = [
            category_data["id"],
            category_data["name"],
            category_data["productCount"],
            category_data["discountedCount"],
            category_data["childCount"],
            category_data["slug"]
        ]
        assert len(category_row) == 6
        categories.append(category_row)
    return categories

def insert_categories(data, categories):
    category_insert_command = """
    INSERT INTO categories VALUES (
        ?,?,?,?,?,?
    )
    """

    category_connect_command = """
    INSERT INTO product_to_categories VALUES (
        ?,?
    )
    """

    for category in categories:
        try:
            cursor.execute(category_insert_command, category)
        except sqlite3.IntegrityError:
            pass
        try:
            cursor.execute(category_connect_command, (data["id"], category[0]))
        except sqlite3.IntegrityError:
            pass

def start_loop():
    error_stats = {
        'no_nutrition': 0,
        'nutrition_wrong_format': 0,
        'index_error': 0,
        'possible_no_brandName': 0,
        'nutrition_found': 0
    }
    # for all subdirectory in base directory: ../rawdata/
    dirs = os.listdir("rawdata/")
    dir_length = len(dirs)
    for i, product in enumerate(dirs):
        # for all files in subdirectory
        print(f'processing {i+1}/{dir_length} id:{product}')
        for variant in os.listdir("rawdata/"+product):
            filename = "rawdata/"+product+"/"+variant+"/"
            with open(filename+"product.json") as f:
                if os.path.exists(filename+"nutrition.json"):
                    error_stats['nutrition_found'] += 1
                data = json.loads(f.read())
                try:
                    variants_data = gather_variants_info(filename, data)
                except FileNotFoundError:
                    print("no nutrition_data", filename)
                    error_stats['no_nutrition'] += 1
                    continue
                except ValueError:
                    print("wrong format", filename)
                    error_stats['nutrition_wrong_format'] += 1
                    continue
                except IndexError:
                    print("index error", filename)
                    error_stats['index_error'] += 1
                    print(filename)
                    break
                try:
                    product_data = construct_product(data)
                except KeyError:
                    print("possible no brandName", filename)
                    error_stats['possible_no_brandName'] += 1
                    break
                insert_product(product_data)
                insert_variant_data(variants_data)
                insert_categories(data, construct_categories(data["categories"]))
    conn.commit()
    print(error_stats)

start_loop()

conn.commit()

# # Problems with nutrition data:
# - some products have no nutrition data
# - some products have nutrition data but it is not in the same format
#     - only kJ or kcal