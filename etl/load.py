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

def polymarket_create_events_table():
    conn, cur = polymarket_connection()
    cur.execute(
    """CREATE TABLE IF NOT EXISTS events (
        event_id INT PRIMARY KEY,
        description TEXT,
        tags TEXT[],
        comments INT,
        start_date TIMESTAMP,
        revelant BOOLEAN);
    """ 
    )
    conn.commit()
    cur.close()
    conn.close()

def polymarket_write_events_table():
    conn, cur = polymarket_connection()
    events = extract.polymarket_get_events()
    seen_ids = set()
    for event in events:
        event_id = transform.polymarket_get_event_id(event)
        description, tags, comments, start_date = transform.polymarket_get_event_details(event)
        seen_ids.add(event_id)
        try:
            cur.execute(
            """
            INSERT INTO events (event_id, description, tags, comments, start_date, revelant) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (event_id) DO UPDATE SET
                description = EXCLUDED.description,
                tags = EXCLUDED.tags,
                comments = EXCLUDED.comments,
                start_date = EXCLUDED.start_date,
                revelant = EXCLUDED.revelant
            """, (event_id, description, tags, comments, start_date, True)
            )
        except Exception as e:
            print(f"Failed to load event{event_id}: {e}")
    cur.execute(
    """
    UPDATE events
    SET revelant = FALSE
    WHERE event_id NOT IN %s
    """, (tuple(seen_ids),)
    )
    conn.commit()
    cur.close()
    conn.close()
    print(f"Data written to events table at {datetime.now(ZoneInfo('America/New_York'))}")

def polymarket_create_markets_table():
    conn, cur = polymarket_connection()
    cur.execute(
    """CREATE TABLE IF NOT EXISTS markets (
        market_id INT PRIMARY KEY,
        event_id INT,
        question TEXT,
        resolved BOOLEAN,
        start_date TIMESTAMP,
        end_date TIMESTAMP,
        revelant BOOLEAN);
    """ 
    )
    conn.commit()
    cur.close()
    conn.close()

def polymarket_write_markets_table():
    conn, cur = polymarket_connection()
    events = extract.polymarket_get_events()
    seen_ids = set()
    for event in events:
        event_id = transform.polymarket_get_event_id(event)
        markets = transform.polymarket_get_markets(event)
        for market in markets:
            market_id = transform.polymarket_get_market_id(market)
            question, resolved, start_date, end_date = transform.polymarket_get_market_details(market)
            seen_ids.add(market_id)
            try:
                cur.execute(
                """
                INSERT INTO markets (market_id, event_id, question, resolved,  start_date, end_date, revelant) VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (market_id) DO UPDATE SET
                    event_id = EXCLUDED.event_id,
                    question = EXCLUDED.question,
                    resolved = EXCLUDED.resolved,
                    start_date = EXCLUDED.start_date,
                    end_date = EXCLUDED.end_date,
                    revelant = EXCLUDED.revelant
                """, (market_id, event_id, question, resolved, start_date, end_date, True)
                )
            except Exception as e:
                print(f"Failed to load market{event_id}: {e}")
    cur.execute(
    """
    UPDATE markets
    SET revelant = FALSE
    WHERE market_id NOT IN %s
    """, (tuple(seen_ids),)
    )
    conn.commit()
    cur.close()
    conn.close()
    print(f"Data written to markets table at {datetime.now(ZoneInfo('America/New_York'))}")

def polymarket_create_snapshots_table():
    conn, cur = polymarket_connection()
    cur.execute(
    """CREATE TABLE IF NOT EXISTS snapshots (
        id SERIAL PRIMARY KEY,
        event_id INT,
        market_id INT,
        resolved BOOLEAN,
        volume FLOAT,
        liquidity FLOAT,
        yes FLOAT,
        no FLOAT,
        last FLOAT,
        bid FLOAT,
        ask FLOAT,
        spread FLOAT, 
        competitive FLOAT,
        timestamp TIMESTAMP,
        latest BOOLEAN,
        UNIQUE (market_id, timestamp));
    """ 
    )
    conn.commit()
    cur.close()
    conn.close()

def polymarket_write_snapshots_table():
    conn, cur = polymarket_connection()
    events = extract.polymarket_get_events()
    seen_ids = set()
    for event in events:
        event_id = transform.polymarket_get_event_id(event)
        markets = transform.polymarket_get_markets(event)
        for market in markets:
            market_id = transform.polymarket_get_market_id(market)
            resolved, volume, liquidity, yes, no, last, bid, ask, spread, competitive = transform.polymarket_get_snapshot_details(market)
            seen_ids.add(market_id)
            try:
                cur.execute(
                """
                INSERT INTO snapshots (event_id, market_id, resolved, volume, liquidity, yes, no, last, bid, ask, spread, competitive, timestamp, latest) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, NOW(), %s)
                """, (event_id, market_id, resolved, volume, liquidity, yes, no, last, bid, ask, spread, competitive, True)
                )
            except Exception as e:
                print(f"Failed to load market snashot{market_id}: {e}")
    cur.execute(
    """
    UPDATE snapshots
    SET latest = FALSE
    WHERE market_id NOT IN %s
    """, (tuple(seen_ids),)
    )
    conn.commit()
    cur.close()
    conn.close()
    print(f"Data written to snapshots table at {datetime.now(ZoneInfo('America/New_York'))}")