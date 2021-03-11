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

channel_id = etl_data_loader.create_channel()
print("Default Channel created")
warehouse_id = etl_data_loader.create_warehouse()
print("Dummy Warehouse created")
shipping_zone_id = etl_data_loader.create_shipping_zone(addWarehouses=[
                                                        warehouse_id])
print("Default shipping zone added")

# qty_attribute_id = etl_data_loader.create_attribute(name="qty")
vendor_attribute_id = etl_data_loader.create_attribute(
    name="vendor", storefrontSearchPosition=0, filterableInStorefront=True, availableInGrid=True, filterableInDashboard=True)
brand_attribute_id = etl_data_loader.create_attribute(
    name="brand", storefrontSearchPosition=1, filterableInStorefront=True)
size_attribute_id = etl_data_loader.create_attribute(
    name="size", filterableInStorefront=False)
unit_size_attribute_id = etl_data_loader.create_attribute(
    name="unit size", filterableInStorefront=False)
print("Basic mandatory attributes added.")

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
print("Created Base product type")

category_id = etl_data_loader.create_category(name="Category")
metadata_keys = ["id", "by_weight", "tags", "department"]
print("Base category added")


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
            description=json.dumps(
                {"blocks": [{"type": "paragraph", "data": {"text": product["description"]}}]}),

            category=category_id,
            attributes=attributes,
        )

        variant_id = etl_data_loader.create_product_variant(
            product_id, sku=product["sku"])
        etl_data_loader.update_product_listing(product_id, channel_id)
        etl_data_loader.update_product_variant_listing(
            variant_id, channel_id, price=product["price"])

        # etl_data_loader.make_product_available(product_id)
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

etl_data_loader.activate_channel(channel_id)
print("Channel Activated!")