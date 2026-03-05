import asyncio
import aiohttp
import logging
from fake_headers import Headers
from src.utils import get_card_url
from tenacity import retry, stop_after_attempt, wait_exponential
import random

HEADER = Headers(
    headers=True
)

SEM = asyncio.Semaphore(20)

async def _proccess_batch(tasks, batch_size: int = 10):
    results = []

    for i in range(0, len(tasks), batch_size):
        batch = tasks[i:i + batch_size]
        batch_results = await asyncio.gather(*batch, return_exceptions=True)
        results.extend(batch_results)

    return results


@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=2, min=2, max=120), reraise=True)
async def _fetch_page(session, query: str, page: int, limit: int):
    async with SEM:
        await asyncio.sleep(random.uniform(0.4, 0.8))
        url = "https://search.wb.ru/exactmatch/ru/common/v4/search"
        params = {
            "resultset": "catalog",
            "sort": "popular",
            "appType": 1,   
            "curr": "rub",
            "dest": -1257786,
            "lang": "ru",
        }
        params["query"] = query
        params["page"] = page
        params["limit"] = limit
        header = HEADER.generate()

        async with session.get(url, params=params, headers=header, timeout=10) as resp:
            resp.raise_for_status()
            data = await resp.json(content_type=None)
            return data

@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=2, min=1, max=32), reraise=True)
async def _fetch_card(session, sku: int, shards: list[str]):
    async with SEM:
        header = HEADER.generate()

        url = get_card_url(sku=sku, shards=shards)

        async with session.get(url, headers=header, timeout=5) as resp:
            resp.raise_for_status()
            data = await resp.json(content_type=None)
            return data

@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=2, min=1, max=120), reraise=True)
async def fetch_shards():
    async with aiohttp.ClientSession() as session:
        url = "https://cdn.wbbasket.ru/api/v3/upstreams"
        header = HEADER.generate()

        async with session.get(url, headers=header) as resp:
            resp.raise_for_status()
            data = await resp.json(content_type=None)

            return data['recommend']['mediabasket_route_map'][0]['hosts']

async def fetch_catalog(query: str, shards: list):
    data = []
    products = []
    page_limit = 100
    async with aiohttp.ClientSession() as session:
        init_page = await _fetch_page(session=session, query=query, page=1, limit=page_limit)
        total = init_page['total']
        products = init_page['products']
        pages = (total + page_limit - 1) // page_limit
        
        page_tasks = [
            _fetch_page(session, query, page, page_limit)
            for page in range(2, pages + 1)
        ]

        pages_results = await _proccess_batch(page_tasks, batch_size=50)

        for i, page_data in enumerate(pages_results, start=2):
            if isinstance(page_data, Exception):
                logging.warning(f"Error while loading page {i}: {page_data}")
            else:
                page_products = page_data.get('products', [])
                products.extend(page_products)

        skus = [product['id'] for product in products]
        card_tasks = [
            _fetch_card(session, sku, shards)
            for sku in skus
        ]

        cards_results = await _proccess_batch(card_tasks, batch_size=50)

        for product, card in zip(products, cards_results):
            if isinstance(card, Exception):
                logging.warning(f"Error while fetching card {product['id']}: {card}")
                data.append({
                    'product': product,
                    'card': None
                })
            else:
                data.append({
                    'product': product,
                    'card': card
                })

    return data
