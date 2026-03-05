def _get_pv(sku: int):
    return sku // 1000, sku // 100000

def _get_host(sku: int, shards: list):
    _, v = _get_pv(sku)

    for shard in shards:
        if shard["vol_range_from"] <= v <= shard["vol_range_to"]:
            return shard["host"]
    raise ValueError('No shards found')

def get_product_url(sku: int): 
    return f'https://www.wildberries.ru/catalog/{sku}/detail.aspx'

def get_card_url(sku: int, shards: list):
    p, v = _get_pv(sku)
    host = _get_host(sku, shards)
    return f"https://{host}/vol{v}/part{p}/{sku}/info/ru/card.json"

def get_image_url(i: int, sku: int, shards: list):
    p, v = _get_pv(sku)
    host = _get_host(sku, shards)
    return f"https://{host}/vol{v}/part{p}/{sku}/images/big/{i}.webp"

def get_seller_url(seller_id: int = None):
    if not seller_id:
        return
    return f"https://www.wildberries.ru/seller/{seller_id}"

