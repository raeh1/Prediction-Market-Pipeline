from dotenv import load_dotenv
from extract import Polymarket_extractor
from transform import Polymarket_transformer
from exceptions import Database_connection_exception
import os, psycopg2
from datetime import datetime
from zoneinfo import ZoneInfo

load_dotenv()

class Loader:
    @classmethod
    def create_connection(cls, host, dbname, user, password, port):
        try:
            conn = psycopg2.connect(
                host = host,
                dbname = dbname,
                user = user,
                password = password,
                port = port
            )
        except Exception:
            raise Database_connection_exception(f"Failed to connect to database {dbname}, verify your credentials!")
        return conn, conn.cursor()

    @classmethod
    def create_table(cls):
        pass

    @classmethod
    def write_table(cls):
        pass

class Polymarket_loader(Loader):
    host = os.getenv("POLYMARKET_HOST")
    dbname = os.getenv("POLYMARKET_DATABASE")
    user = os.getenv("POLYMARKET_USER")
    password = os.getenv("POLYMARKET_PASSWORD")
    port = os.getenv("POLYMARKET_PORT")

    @classmethod
    def create_connection(cls):
        return super().create_connection(cls.host, cls.dbname, cls.user, cls.password, cls.port)
    
    @classmethod
    def create_events_table(cls):
        conn, cur = cls.create_connection()
        cur.execute(
        """CREATE TABLE IF NOT EXISTS events (
            event_id INT PRIMARY KEY,
            title TEXT,
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

    @classmethod
    def create_markets_table(cls):
        conn, cur = cls.create_connection()
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

    @classmethod
    def create_snapshots_table(cls):
        conn, cur = cls.create_connection()
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

    @classmethod
    def write_events_table(cls):
        conn, cur = cls.create_connection()
        events = Polymarket_extractor.get_events()
        seen_ids = set()
        for event in events:
            event_id = Polymarket_transformer.get_event_id(event)
            title, description, tags, comments, start_date = Polymarket_transformer.get_event_details(event)
            seen_ids.add(event_id)
            try:
                cur.execute(
                """
                INSERT INTO events (event_id, title, description, tags, comments, start_date, revelant) VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (event_id) DO UPDATE SET
                    title = EXCLUDED.title,
                    description = EXCLUDED.description,
                    tags = EXCLUDED.tags,
                    comments = EXCLUDED.comments,
                    start_date = EXCLUDED.start_date,
                    revelant = EXCLUDED.revelant
                """, (event_id, title, description, tags, comments, start_date, True)
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

    @classmethod
    def write_markets_table(cls):
        conn, cur = cls.create_connection()
        events = Polymarket_extractor.get_events()
        seen_ids = set()
        for event in events:
            event_id = Polymarket_transformer.get_event_id(event)
            markets = Polymarket_transformer.get_markets(event)
            for market in markets:
                market_id = Polymarket_transformer.get_market_id(market)
                question, resolved, start_date, end_date = Polymarket_transformer.get_market_details(market)
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

    @classmethod
    def write_snapshots_table(cls):
        conn, cur = cls.create_connection()
        events = Polymarket_extractor.get_events()
        seen_ids = set()
        for event in events:
            event_id = Polymarket_transformer.get_event_id(event)
            markets = Polymarket_transformer.get_markets(event)
            for market in markets:
                market_id = Polymarket_transformer.get_market_id(market)
                resolved, volume, liquidity, yes, no, last, bid, ask, spread, competitive = Polymarket_transformer.get_snapshot_details(market)
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