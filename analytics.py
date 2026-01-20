import psycopg2
import pandas as pd

def get_results(query):
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=postgres password=8484123")
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def run_analytics():
    print("--- 📊 SPARKIFY ANALYTICS DASHBOARD 📊 ---\n")

    # Question 1: What are the top 5 most played songs?
    # (Note: In generated data, 'title' might be 'Song_Title_X')
    q1 = """
    SELECT s.title, COUNT(*) as play_count
    FROM songplays sp
    JOIN songs s ON sp.song_id = s.song_id
    GROUP BY s.title
    ORDER BY play_count DESC
    LIMIT 5;
    """
    print("1. TOP 5 PLAYED SONGS:")
    print(get_results(q1))
    print("-" * 30)

    # Question 2: When is the app most active? (Usage by Hour)
    q2 = """
    SELECT time.hour, COUNT(*) as activity_count
    FROM songplays sp
    JOIN time ON sp.start_time = time.start_time
    GROUP BY time.hour
    ORDER BY activity_count DESC
    LIMIT 5;
    """
    print("\n2. BUSIEST HOURS OF THE DAY:")
    print(get_results(q2))
    print("-" * 30)

    # Question 3: User Level Analysis (Free vs Paid)
    q3 = """
    SELECT level, COUNT(DISTINCT user_id) as user_count
    FROM users
    GROUP BY level;
    """
    print("\n3. USER BASE BREAKDOWN (FREE vs PAID):")
    print(get_results(q3))
    print("-" * 30)
    
    # Question 4: Who are the most active users?
    q4 = """
    SELECT u.first_name, u.last_name, COUNT(*) as total_listens
    FROM songplays sp
    JOIN users u ON sp.user_id = u.user_id
    GROUP BY u.first_name, u.last_name
    ORDER BY total_listens DESC
    LIMIT 5;
    """
    print("\n4. SUPER USERS (MOST ACTIVE):")
    print(get_results(q4))
    print("-" * 30)

if __name__ == "__main__":
    run_analytics()