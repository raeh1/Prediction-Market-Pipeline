import concurrent.futures, load

def main():
    with concurrent.futures.ThreadPoolExecutor() as executor1:
        futures1 = [
            executor1.submit(load.polymarket_create_markets_table),
            executor1.submit(load.polymarket_create_snapshots_table)
        ]
        for future in futures1:
            try:
                future.result()
            except Exception as e:
                print(f"Error during writing: {e}")

    with concurrent.futures.ThreadPoolExecutor() as executor2:
        futures2 = [
            executor2.submit(load.polymarket_write_markets_table),
            executor2.submit(load.polymarket_write_snapshots_table)
        ]
        for future in futures2:
            try:
                future.result()
            except Exception as e:
                print(f"Error during writing: {e}")

if __name__ == "__main__":
    main()