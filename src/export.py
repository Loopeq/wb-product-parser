from pathlib import Path

import pandas as pd

STORAGE_PATH = Path(__file__).resolve().parent.parent / "storage"

COLUMN_MAPPING = {
    'product_url': 'Ссылка на товар',
    'sku': "Артикул",
    'title': "Название",
    'price': 'Цена',
    'description': "Описание",
    'options': 'Характеристики товара',
    'image_urls': 'Ссылки на изображения',
    'seller_name': 'Название селлера',
    'seller_url': 'Ссылка на селлера',
    'sizes': 'Размеры товаров',
    'quantity': 'Остатки',
    'rating': 'Рейтинг',
    'feedbacks': 'Количество отзывов',
    'country': 'Страна производства',
}

def export(rows: list[dict], filename: str):
    df = pd.DataFrame(rows)

    df = df.rename(columns=COLUMN_MAPPING)

    STORAGE_PATH.mkdir(exist_ok=True)

    output = STORAGE_PATH / filename
    df.to_excel(output, engine='openpyxl', index=False)