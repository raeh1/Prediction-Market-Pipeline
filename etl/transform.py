from dateutil.parser import parse
import json

class Transformer:
    @classmethod
    def get_details(cls):
        pass

class Polymarket_transformer(Transformer):
    @classmethod
    def get_event_id(cls, event):
        return int(event.get("id"))

    @classmethod
    def get_market_id(cls, market):
        return int(market.get("id"))

    @classmethod
    def get_markets(cls, event):
        return event.get("markets")

    @classmethod
    def get_event_details(cls, event):
        title = event.get("title")
        description = event.get("description")
        tags = []
        all_tags = event.get("tags")
        for tag in all_tags:
            tags.append(tag.get("label"))
        comments = event.get("commentCount")
        start_date = event.get("startDate")
        if start_date:
            start_date = parse(start_date)
        return title, description, tags, comments, start_date

    @classmethod
    def get_market_details(cls, market):
        question = market.get("question")
        resolved = market.get("umaResolutionStatus") == "resolved"
        start_date = market.get("startDate")
        if start_date:
            start_date = parse(start_date)
        end_date = market.get("endDate")
        if end_date:
            end_date = parse(end_date)
        return question, resolved, start_date, end_date

    @classmethod
    def get_snapshot_details(cls, market):
        resolved = market.get("umaResolutionStatus") == "resolved"
        volume = market.get("volumeNum")
        liquidity = market.get("liquidityNum")
        yes, no = None, None
        if (market.get("outcomePrices")):
            prices = [float(x) for x in json.loads(market.get("outcomePrices"))]
            yes = prices[0]
            no = prices[1]
        last = market.get("lastTradePrice")
        bid = market.get("bestBid")
        ask = market.get("bestAsk")   
        spread = market.get("spread")
        competitive = market.get("competitive")
        return resolved, volume, liquidity, yes, no, last, bid, ask, spread, competitive