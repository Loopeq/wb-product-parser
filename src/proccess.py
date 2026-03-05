import pandas as pd
from datetime import datetime
from src.utils import get_image_url, get_product_url, get_seller_url
from pathlib import Path

STORAGE_PATH = Path(__file__).resolve().parent.parent / "storage"

def process_catalog(catalog: list[dict], shards: list, filename: str = None):
    rows = []

    for item in catalog:
        product = item.get('product') or {}
        card = item.get('card') or {}

        sku = product.get('id', None)

        link = None
        if sku:
            link = get_product_url(sku)

        title = product.get('name', '')

        basic_price = None
        price = product.get('sizes', [])
        if len(price):
            basic_price = price[0].get('price', {}).get('product', None)
        if (basic_price): 
            basic_price = basic_price / 100

        description = card.get('description')

        full_colors = card.get('full_colors', [])
        imgs = ', '.join([get_image_url(i=i, sku=sku, shards=shards) for i in range(len(full_colors))])
        
        options = card.get('options', [])

        seller_name = product.get('supplier', '')

        seller_id = product.get('supplierId', '')
        seller_link = get_seller_url(seller_id=seller_id)

        sizes = product.get('sizes', [])
        size_string = ', '.join([size.get('name') for size in sizes])

        quantity = product.get('totalQuantity', None)
        rating = product.get('reviewRating', None)
        feedbacks = product.get('nmFeedbacks', None)

        row = {
            'Ссылка на товар': link,
            'Артикул': sku,
            'Название': title,
            'Цена': basic_price,
            'Описание': description,
            'Ссылки на изображения': imgs,
            'Характеристики': options,
            'Название селлера': seller_name,
            'Ссылка на селлера': seller_link,
            'Размеры товара': size_string,
            'Остатки по товару': quantity,
            'Рейтинг': rating,
            'Количество отзывов': feedbacks
        }

        rows.append(row)

    df = pd.DataFrame(rows)

    if not filename:
        today = datetime.now().strftime('%Y-%m-%d')
        filename = f"products_{today}.xlsx"

    STORAGE_PATH.mkdir(exist_ok=True)
    output = STORAGE_PATH / filename

    df.to_excel(output, engine='openpyxl')