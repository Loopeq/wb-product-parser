
import asyncio
from src.client import fetch_catalog, fetch_shards
from src.normalize import normalize_catalog, filter_catalog
from src.export import export

async def run(query: str):
    shards = await fetch_shards()
    catalog = await fetch_catalog(query=query, shards=shards)
    
    rows = normalize_catalog(catalog, shards)
    export(rows, 'catalog_base.xlsx')

    filters = {
        'rating': {"gte": 4.5},
        'price': {'lte': 10000},
        'country': {'eq': 'Россия'}
    }

    rows = filter_catalog(rows, filters)

    export(rows, "catalog_filtered.xlsx")
        
async def main():
    await run(query='пальто из натуральной шерсти')
    
if __name__ == "__main__": 
    asyncio.run(main())