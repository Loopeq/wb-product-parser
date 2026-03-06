from pathlib import Path

import pandas as pd

from src.utils import get_image_url, get_product_url, get_seller_url

STORAGE_PATH = Path(__file__).resolve().parent.parent / "storage"

def normalize_catalog(catalog: list[dict], shards: list):
    rows = []

    for item in catalog:
        product = item.get('product') or {}
        card = item.get('card') or {}

        sku = product.get('id')

        link = get_product_url(sku) if sku else None
        title = product.get('name')

        price = None
        size_string = None
        sizes = product.get('sizes', [])

        if sizes:
            price = sizes[0].get('price', {}).get('product')
            size_string = ", ".join(size.get("name") for size in sizes)

        if price:
            price /= 100

        full_colors = card.get('full_colors', [])

        imgs = ", ".join(
            get_image_url(i=i, sku=sku, shards=shards)
            for i in range(1, len( full_colors) - 1)
        )

        seller_id = product.get("supplierId")
        
        options = card.get('options', [])
        country = next((opt['value'] for opt in options if opt['name'] == 'Страна производства'), None)

        row = {
            'product_url': link,
            'sku': sku,
            'title': title,
            'price': price,
            'description': card.get('description'),
            'image_urls': imgs,
            'options': options,
            'sizes': size_string,
            'seller_name': product.get('supplier'),
            'seller_url': get_seller_url(seller_id),
            'feedbacks': product.get('nmFeedbacks'),
            'quantity': product.get('totalQuantity'),
            'rating': product.get('reviewRating'),
            'country': country,
        }

        rows.append(row)

    return rows

def filter_catalog(rows: list[dict], filters: dict):
    df = pd.DataFrame(rows)

    for field, condition in filters.items():
        if 'gte' in condition:
            df = df[df[field] >= condition['gte']]
        if 'lte' in condition:
            df = df[df[field] <= condition['lte']]
        if 'eq' in condition:
            df = df[df[field] == condition['eq']]


    return df.to_dict("records")