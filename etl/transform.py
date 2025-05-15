from dateutil.parser import parse
import json

def polymarket_get_event_details(event):
    event_id = int(event.get("id"))
    markets = event.get("markets")
    tags = []
    all_tags = event.get("tags")
    for tag in all_tags:
        tags.append(tag.get("label"))
    return event_id, markets, tags

def polymarket_get_market_details(market):
    market_id = int(market.get("id"))
    question = market.get("question")
    start_date = market.get("startDate")
    if start_date:
        start_date = parse(start_date)
    end_date = market.get("endDate")
    if end_date:
        end_date = parse(end_date)
    return market_id, question, start_date, end_date

def polymarket_get_market_snapshots(market):
    market_id = int(market.get("id"))

    price_yes, price_no = None, None
    if (market.get("outcomePrices")):
        prices = [float(x) for x in json.loads(market.get("outcomePrices"))]
        price_yes = prices[0]
        price_no = prices[1]
    last_price = market.get("lastTradePrice")
    bid = market.get("bestBid")
    ask = market.get("bestAsk")
    volume = market.get("volumeNum")
    liquidity = market.get("liquidityNum")
    spread = market.get("spread")
    competitive = market.get("competitive")
    return market_id, price_yes, price_no, last_price, bid, ask, volume, liquidity, spread, competitive