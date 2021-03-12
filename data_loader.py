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
    # {"key": "vendor", "key_id": vendor_attribute_id},
    {"key": "brand", "key_id": brand_attribute_id},
    {"key": "size", "key_id": size_attribute_id},
    {"key": "unit_size", "key_id": unit_size_attribute_id},
]

product_type_id = etl_data_loader.create_product_type(
    name="Product",
    hasVariants=True,
    productAttributes=[brand_attribute_id,
                       size_attribute_id, unit_size_attribute_id],
    variantAttributes=[vendor_attribute_id]
)
print("Created Base product type")

category_id = etl_data_loader.create_category(name="Category")
metadata_keys = ["by_weight", "department"]
print("Base category added")

result = {}

for product in data:
    if product.get('vendor') in [pro['vendor'] for pro in result.get(product.get("sku"), [])]:
        continue
    result[product.get("sku")] = [*result.get(product.get("sku"), []), product]


def add_product_details(sku_variants):
    product = sku_variants[0]
    metadata = []
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
        product_id = etl_data_loader.create_product(product_type_id, name=product["name"], description=json.dumps(
            {"blocks": [{"type": "paragraph", "data": {"text": product["description"]}}]}), category=category_id, attributes=attributes)
        etl_data_loader.add_product_metadata(
            product_id, metadata)

        etl_data_loader.update_product_listing(product_id, channel_id)
        for variant in sku_variants:
            if not product.get("image") and variant.get("image"):
                product['image'] = variant['image']

            if not variant.get("price"):
                continue

            variant['price'] = round(float(variant.get('price')), 2)

            variant_meta_data = [{"key": "original_url", "value": variant["url"]}, {
                "key": "original_id", "value": variant["id"]}, {"key": "tags", "value": variant.get("tags", "null")}, ]

            variant_id = etl_data_loader.create_product_variant(
                product_id, sku=f"{variant['sku']}-{variant['vendor']}", attributes=[{"id": vendor_attribute_id, "values": variant.get("vendor")}], )
            etl_data_loader.update_product_variant_listing(
                variant_id, channel_id, price=variant["price"])
            etl_data_loader.add_product_metadata(variant_id, variant_meta_data)
        # etl_data_loader.make_product_available(product_id)

        url = product["image"]
        response = requests.get(url)

        with tempfile.NamedTemporaryFile(mode="wb", delete=True, suffix='.jpg') as image_file:
            image_file.write(response.content)
            response.raw.decode_content = True
            etl_data_loader.create_product_image(product_id, image_file.name)

        print(
            f"SUCCESS Added product:  id: {product['id']} Name: {product['name']}")
    except Exception:
        print(
            f"ERROR adding product:  id: {product['id']} Name: {product['name']}")


with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
    executor.map(add_product_details, list(result.values()))


etl_data_loader.activate_channel(channel_id)
print("Channel Activated!")
