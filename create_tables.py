import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    """
    - Creates and connects to the sparkifydb
    - Returns the cursor and connection to the new database
    """
    
    # 1. Connect to the default 'postgres' database first
    # CHANGE 'user' and 'password' below to match your local setup
    conn = psycopg2.connect("host=127.0.0.1 dbname=postgres user=postgres password=8484123")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # 2. Drop the project database if it already exists (Clean Slate)
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    
    # 3. Create the new project database
    cur.execute("CREATE DATABASE sparkifydb")
    
    # 4. Close connection to default database
    conn.close()
    
    # 5. Connect to the newly created 'sparkifydb'
    # CHANGE 'user' and 'password' below to match your local setup
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=postgres password=8484123")
    cur = conn.cursor()
    
    return cur, conn


def drop_tables(cur, conn):
    """
    Runs the drop queries defined in sql_queries.py
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Runs the create queries defined in sql_queries.py
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Drops (if exists) and Creates the sparkify database. 
    - Establishes connection with the sparkify database and gets 
      cursor to it.  
    - Drops all the tables.  
    - Creates all tables needed. 
    - Finally, closes the connection. 
    """
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()