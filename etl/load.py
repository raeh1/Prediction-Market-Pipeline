from dotenv import load_dotenv
import os, psycopg2
import extract, transform
from datetime import datetime
from zoneinfo import ZoneInfo

load_dotenv()

def polymarket_connection():
    conn = psycopg2.connect(
        host = os.getenv("POLYMARKET_HOST"),
        dbname = os.getenv("POLYMARKET_DATABASE"),
        user = os.getenv("POLYMARKET_USER"),
        password = os.getenv("POLYMARKET_PASSWORD"),
        port = os.getenv("POLYMARKET_PORT")
    )
    return conn, conn.cursor()

def polymarket_create_markets_table():
    conn, cur = polymarket_connection()
    cur.execute(
    """CREATE TABLE IF NOT EXISTS markets (
        market_id INT PRIMARY KEY,
        event_id INT,
        question TEXT,
        tags TEXT[],
        active BOOLEAN,
        last_seen TIMESTAMP,
        start_date TIMESTAMP,
        end_date TIMESTAMP);
    """ 
    )
    conn.commit()
    cur.close()
    conn.close()

def polymarket_create_snapshots_table():
    conn, cur = polymarket_connection()
    cur.execute(
    """CREATE TABLE IF NOT EXISTS snapshots (
        id SERIAL PRIMARY KEY,
        market_id INT,
        timestamp TIMESTAMP,
        price_yes FLOAT,
        price_no FLOAT,
        last_price FLOAT,
        bid FLOAT,
        ask FLOAT,
        volume FLOAT,
        liquidity FLOAT,
        spread FLOAT, 
        competitive FLOAT,
        active BOOLEAN,
        UNIQUE (market_id, timestamp));
    """ 
    )
    conn.commit()
    cur.close()
    conn.close()

def polymarket_write_markets_table():
    conn, cur = polymarket_connection()
    events = extract.get_events()
    seen_ids = set()
    for event in events:
        event_id, markets, tags = transform.polymarket_get_event_details(event)
        for market in markets:
            market_id, question, start_date, end_date = transform.polymarket_get_market_details(market)
            seen_ids.add(market_id)
            try:
                cur.execute(
                """
                INSERT INTO markets (market_id, event_id, question, tags, active, last_seen, start_date, end_date) VALUES (%s, %s, %s, %s, True, NOW(), %s, %s) ON CONFLICT (market_id) DO UPDATE SET active = TRUE, last_seen = NOW()
                """, (market_id, event_id, question, tags, start_date, end_date)
                )
            except Exception as e:
                print(f"Failed to load market{market_id}: {e}")
    cur.execute(
    """
    UPDATE markets
    SET active = FALSE
    WHERE market_id NOT IN %s
    """, (tuple(seen_ids),)
    )
    conn.commit()
    cur.close()
    conn.close()
    print(f"Data written to markets table at {datetime.now(ZoneInfo('America/New_York'))}")

def polymarket_write_snapshots_table():
    conn, cur = polymarket_connection()
    events = extract.get_events()
    seen_ids = set()
    for event in events:
        event_id, markets, tags = transform.polymarket_get_event_details(event)
        for market in markets:
            market_id, price_yes, price_no, last_price, bid, ask, volume, liquidity, spread, competitive = transform.polymarket_get_market_snapshots(market)
            seen_ids.add(market_id)
            try:
                cur.execute(
                """
                INSERT INTO snapshots (market_id, timestamp, price_yes, price_no, last_price, bid, ask, volume, liquidity, spread, competitive, active) VALUES (%s, NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s, True)
                """, (market_id, price_yes, price_no, last_price, bid, ask, volume, liquidity, spread, competitive)
                )
            except Exception as e:
                print(f"Failed to load market{market_id}: {e}")
    cur.execute(
    """
    UPDATE snapshots
    SET active = FALSE
    WHERE market_id NOT IN %s
    """, (tuple(seen_ids),)
    )
    conn.commit()
    cur.close()
    conn.close()
    print(f"Data written to snapshots table at {datetime.now(ZoneInfo('America/New_York'))}")