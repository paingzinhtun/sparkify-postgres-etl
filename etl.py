import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    1. Reads a song JSON file into a Pandas DataFrame.
    2. Extracts song data and inserts it into the `songs` table.
    3. Extracts artist data and inserts it into the `artists` table.
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # INSERT SONG RECORD
    # Select columns: song_id, title, artist_id, year, duration
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # INSERT ARTIST RECORD
    # Select columns: artist_id, name, location, latitude, longitude
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    1. Reads a log JSON file.
    2. Filters for 'NextSong' actions.
    3. Transforms timestamps into hour, day, week, etc., for the `time` table.
    4. Extracts user info for the `users` table.
    5. Performs a lookup to find song_id and artist_id, then inserts into `songplays`.
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # CONVERT TIMESTAMP TO DATETIME
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # INSERT TIME RECORDS
    # Extract time data: start_time, hour, day, week, month, year, weekday
    time_data = (t, t.dt.hour, t.dt.day, t.dt.isocalendar().week, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    
    # Create a dictionary to construct the DataFrame easily
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # INSERT USER RECORDS
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # INSERT SONGPLAY RECORDS
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        # We need to run the SELECT query we wrote in sql_queries.py
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Iterates over all files in the directory `filepath` and executes 
    the processing function `func` (either process_song_file or process_log_file) on them.
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    # CHANGE 'user' and 'password' below to match your local setup
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=postgres password=8484123")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()