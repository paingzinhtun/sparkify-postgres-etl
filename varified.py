import psycopg2
import pandas as pd

def verify():
    # Connect to your new database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=postgres password=8484123")
    cur = conn.cursor()

    print("--- 1. CHECKING USERS ---")
    cur.execute("SELECT COUNT(*) FROM users;")
    print(f"Total Users: {cur.fetchone()[0]}")

    print("\n--- 2. CHECKING SONGS ---")
    cur.execute("SELECT COUNT(*) FROM songs;")
    print(f"Total Songs: {cur.fetchone()[0]}")

    print("\n--- 3. CHECKING MATCH LOGIC ---")
    # This is the most important check. 
    # Did we successfully link a log event to a song ID?
    query = """
        SELECT count(*) 
        FROM songplays 
        WHERE song_id IS NOT NULL;
    """
    cur.execute(query)
    matches = cur.fetchone()[0]
    print(f"Songplays with successful matches (NOT NULL song_id): {matches}")
    
    if matches > 0:
        print("SUCCESS: Your ETL pipeline is correctly matching songs!")
    else:
        print("WARNING: No matches found. (This is expected if random data didn't align, but check logic if using real data)")

    conn.close()

if __name__ == "__main__":
    verify()