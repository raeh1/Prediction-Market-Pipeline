from exceptions import *
import requests
from urllib.parse import urljoin

gammaAPI = "https://gamma-api.polymarket.com/"
urlModifier = "events?active=true&closed=false&limit=500&sort=volume"

def get_events():
    events = requests.get(urljoin(gammaAPI, urlModifier))
    if not events.ok:
        raise EventsFetchException(f"Failed to fetch events: {events.status_code} - {events.text}")
    return events.json()