import concurrent.futures, load

def main():
    with concurrent.futures.ThreadPoolExecutor() as executor1:
        executor1.submit(load.polymarket_create_markets_table)
        executor1.submit(load.polymarket_create_snapshots_table)

    with concurrent.futures.ThreadPoolExecutor() as executor2:
        executor2.submit(load.polymarket_write_markets_table)
        executor2.submit(load.polymarket_write_snapshots_table) 

if __name__ == "__main__":
    main()