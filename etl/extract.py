import requests
from urllib.parse import urljoin
from exceptions import Fetch_event_exception

class Extractor:
    @classmethod
    def get_events(cls):
        pass

class Polymarket_extractor(Extractor):
    polymarket_gammaAPI = "https://gamma-api.polymarket.com/"
    polymarket_urlModifier = "events?active=true&closed=false&limit=500&order=volume&ascending=false"    # top 500 events in volume
    
    @classmethod
    def get_events(cls):
        events = requests.get(urljoin(cls.polymarket_gammaAPI, cls.polymarket_urlModifier))
        if not events.ok:
            raise Fetch_event_exception(f"Failed to fetch Polymarket events: {events.status_code} - {events.text}")
        return events.json()