
import asyncio
from src.client import fetch_catalog, fetch_shards
from src.proccess import process_catalog 

async def search(query: str, filename: str = None, **filters):
    shards = await fetch_shards()
    catalog = await fetch_catalog(query=query, shards=shards)
    process_catalog(catalog=catalog, shards=shards, filename=filename)
    
async def main():
    await search(query='пальто из натуральной шерсти', filename='base_catalog.xlsx')
    
if __name__ == "__main__": 
    asyncio.run(main())