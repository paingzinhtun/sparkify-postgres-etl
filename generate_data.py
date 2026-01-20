import os
import json
import random
import time
from datetime import datetime, timedelta

def create_directory_structure():
    """Creates the data directories if they don't exist."""
    os.makedirs('data/song_data', exist_ok=True)
    os.makedirs('data/log_data', exist_ok=True)
    print("Created directories: data/song_data and data/log_data")

def generate_song_data(num_songs=10):
    """Generates fake song JSON files."""
    songs = []
    print(f"Generating {num_songs} songs...")
    
    for i in range(num_songs):
        song_id = f"SONG_{i}"
        artist_id = f"ARTIST_{i}"
        
        song = {
            "num_songs": 1,
            "artist_id": artist_id,
            "artist_latitude": random.uniform(-90, 90),
            "artist_longitude": random.uniform(-180, 180),
            "artist_location": f"City_{i}",
            "artist_name": f"Artist_{i}",
            "song_id": song_id,
            "title": f"Song_Title_{i}",
            "duration": round(random.uniform(120, 300), 2),
            "year": random.randint(1990, 2024)
        }
        songs.append(song)
        
        # Save each song to a separate JSON file (mimicking the real dataset)
        filename = f"data/song_data/song_{i}.json"
        with open(filename, 'w') as f:
            json.dump(song, f)
            
    return songs

def generate_log_data(songs, num_entries=50):
    """
    Generates fake log JSON files.
    IMPORTANT: We intentionally use songs from the list above 
    so our ETL pipeline finds matches.
    """
    print(f"Generating {num_entries} log entries...")
    
    logs = []
    start_time = datetime.now()
    
    for i in range(num_entries):
        # 50% chance to listen to a known song (to test the JOIN match)
        if random.random() > 0.5 and songs:
            matched_song = random.choice(songs)
            song_title = matched_song['title']
            artist_name = matched_song['artist_name']
            length = matched_song['duration']
        else:
            # Random song (will result in NULL in songplay table)
            song_title = f"Random_Song_{i}"
            artist_name = f"Random_Artist_{i}"
            length = 200.0

        # Create timestamp in milliseconds
        ts = int((start_time + timedelta(minutes=i)).timestamp() * 1000)
        
        log_entry = {
            "artist": artist_name,
            "auth": "Logged In",
            "firstName": f"UserFirst_{random.randint(1,5)}",
            "gender": random.choice(["M", "F"]),
            "itemInSession": i,
            "lastName": f"UserLast_{random.randint(1,5)}",
            "length": length,
            "level": random.choice(["free", "paid"]),
            "location": "San Francisco-Oakland-Hayward, CA",
            "method": "PUT",
            "page": "NextSong", # Important filter for our ETL
            "registration": 1540000000000,
            "sessionId": 123,
            "song": song_title,
            "status": 200,
            "ts": ts,
            "userAgent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4)",
            "userId": str(random.randint(1, 10))
        }
        logs.append(log_entry)

    # Save all logs into one file (simulating a daily log file)
    # Note: Sparkify log files are "JSON Lines" (one JSON object per line)
    with open('data/log_data/2026-01-01-events.json', 'w') as f:
        for entry in logs:
            f.write(json.dumps(entry) + '\n')

if __name__ == "__main__":
    create_directory_structure()
    generated_songs = generate_song_data()
    generate_log_data(generated_songs)
    print("Data generation complete! You are ready to run ETL.")