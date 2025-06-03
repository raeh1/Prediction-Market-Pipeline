from load import Polymarket_loader
import concurrent.futures

def main():
    with concurrent.futures.ThreadPoolExecutor() as executor1:
        futures1 = [
            executor1.submit(Polymarket_loader.create_events_table),
            executor1.submit(Polymarket_loader.create_markets_table),
            executor1.submit(Polymarket_loader.create_snapshots_table)
        ]
        for future in futures1:
            try:
                future.result()
            except Exception as e:
                print(f"Error during creating table: {e}")

    with concurrent.futures.ThreadPoolExecutor() as executor2:
        futures2 = [
            executor2.submit(Polymarket_loader.write_events_table),
            executor2.submit(Polymarket_loader.write_markets_table),
            executor2.submit(Polymarket_loader.write_snapshots_table)
        ]
        for future in futures2:
            try:
                future.result()
            except Exception as e:
                print(f"Error during writing to table: {e}")

if __name__ == "__main__":
    main()