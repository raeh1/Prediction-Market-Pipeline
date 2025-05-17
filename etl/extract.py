import exceptions
import requests
from urllib.parse import urljoin

polymarket_gammaAPI = "https://gamma-api.polymarket.com/"

# top 500 events in volume
polymarket_urlModifier = "events?active=true&closed=false&limit=500&order=volume&ascending=false"

def polymarket_get_events():
    events = requests.get(urljoin(polymarket_gammaAPI, polymarket_urlModifier))
    if not events.ok:
        raise exceptions.EventsFetchException(f"Failed to fetch events: {events.status_code} - {events.text}")
    return events.json()