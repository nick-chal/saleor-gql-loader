import os
import json
import requests
import tempfile
import concurrent
from uuid import uuid4
from dotenv import load_dotenv

from saleor_gql_loader import ETLDataLoader

load_dotenv()

with open("algolia-dump.json", "r") as data_file:
    data = json.load(data_file)


etl_data_loader = ETLDataLoader(
    os.environ["APP_AUTH_TOKEN"], endpoint_url=os.environ["GRAPHQL_URL"])

warehouse_id = etl_data_loader.create_warehouse()
shipping_zone_id = etl_data_loader.create_shipping_zone(addWarehouses=[
                                                        warehouse_id])

# qty_attribute_id = etl_data_loader.create_attribute(name="qty")
vendor_attribute_id = etl_data_loader.create_attribute(
    name="vendor", storefrontSearchPosition=0)
brand_attribute_id = etl_data_loader.create_attribute(
    name="brand", storefrontSearchPosition=1)
size_attribute_id = etl_data_loader.create_attribute(
    name="size", filterableInStorefront=False)
unit_size_attribute_id = etl_data_loader.create_attribute(
    name="unit size", filterableInStorefront=False)

attribute_keys = [
    {"key": "vendor", "key_id": vendor_attribute_id},
    {"key": "brand", "key_id": brand_attribute_id},
    {"key": "size", "key_id": size_attribute_id},
    {"key": "unit_size", "key_id": unit_size_attribute_id},
]

product_type_id = etl_data_loader.create_product_type(
    name="Product",
    hasVariants=False,
    productAttributes=[vendor_attribute_id, brand_attribute_id,
                       size_attribute_id, unit_size_attribute_id],
)

category_id = etl_data_loader.create_category(name="Category")
metadata_keys = ["id", "by_weight", "tags", "department"]


def add_product_details(product):
    metadata = [{"key": "original_url", "value": product["url"]}]
    for key in metadata_keys:
        if product.get(key) is None:
            continue
        metadata.append({"key": key, "value": str(product.get(key))})

    attributes = []
    for key in attribute_keys:
        if not product.get(key["key"]):
            continue
        attributes.append(
            {"id": key["key_id"], "values": [str(product.get(key["key"]))]})

    try:

        product_id = etl_data_loader.create_product(
            product_type_id,
            name=product["name"],
            descriptionJson=json.dumps({"blocks": [{"key": str(uuid4())[:5], "text": product["description"], "type": "unstyled",
                                                    "depth": 0, "inlineStyleRanges": [], "entityRanges":[], "data":{}}], "entityMap": {}}),
            basePrice=product["price"],
            sku=product["sku"],
            category=category_id,
            attributes=attributes,
            isPublished=True,
            publicationDate='2021-03-03',
            visibleInListings=True,
        )

        etl_data_loader.make_product_available(product_id)
        etl_data_loader.add_product_metadata(
            product_id, metadata)

        url = product["image"]
        response = requests.get(url)

        with tempfile.NamedTemporaryFile(mode="wb", delete=True, suffix='.jpg') as image_file:
            image_file.write(response.content)
            response.raw.decode_content = True
            etl_data_loader.create_product_image(product_id, image_file.name)

        print(
            f"SUCCESS Added product:  id: {product['id']} Name: {product['name']}")
    except Exception as e:

        print(
            f"ERROR adding product:  id: {product['id']} Name: {product['name']}")


with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
    executor.map(add_product_details, data)
