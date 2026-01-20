# 🎵 Sparkify Data Modeling with Postgres

## 1. Project Overview
**Sparkify** is a startup that wants to analyze the data they’ve been collecting on songs and user activity from their new music streaming app. The analytics team is particularly interested in understanding **what songs users are listening to**.

Currently, Sparkify’s data exists as:
* **JSON logs** containing user activity
* **JSON files** containing song metadata

Because the data is stored as raw JSON files, it is difficult to query and analyze efficiently.

### 🚀 Project Goal
The goal of this project is to:
* **Design a PostgreSQL database** optimized for analytical queries.
* **Implement an ETL pipeline** using Python and Pandas.
* **Enable Sparkify’s analytics team** to easily query song play data.

---

## 2. Database Schema Design (Star Schema)
This project uses a **Star Schema**, which is a common data modeling approach for analytics (OLAP workloads).

### Why Star Schema?
* **Simplicity:** Queries are easier to write with fewer joins.
* **Performance:** Faster aggregations for analytics use cases.
* **Industry Standard:** Widely used in data warehouses.

### ⭐ Fact Table

**`songplays`** Records every time a user plays a song. This table is the center of analysis, used for counting and aggregating song plays.

| Column | Description |
| :--- | :--- |
| `songplay_id` | Primary key |
| `start_time` | Timestamp of the song play |
| `user_id` | User who played the song |
| `level` | User subscription level |
| `song_id` | Song played |
| `artist_id` | Artist of the song |
| `session_id` | User session |
| `location` | User location |
| `user_agent` | User browser/device info |

### 📐 Dimension Tables

**`users`** Stores user information.
| Column | Description |
| :--- | :--- |
| `user_id` | Primary key |
| `first_name` | User first name |
| `last_name` | User last name |
| `gender` | User gender |
| `level` | Subscription level |

**`songs`** Stores song metadata.
| Column | Description |
| :--- | :--- |
| `song_id` | Primary key |
| `title` | Song title |
| `artist_id` | Artist ID |
| `year` | Release year |
| `duration` | Song length |

**`artists`** Stores artist information.
| Column | Description |
| :--- | :--- |
| `artist_id` | Primary key |
| `name` | Artist name |
| `location` | Artist location |
| `latitude` | Latitude |
| `longitude` | Longitude |

**`time`** Stores time breakdowns for analytics.
| Column | Description |
| :--- | :--- |
| `start_time` | Timestamp |
| `hour` | Hour |
| `day` | Day |
| `week` | Week |
| `month` | Month |
| `year` | Year |
| `weekday` | Day of the week |

> **🔎 Why a Time Table?** > Pre-calculating time components allows analysts to easily group by hour, day, or week without expensive date functions in queries.

---

## 3. ETL Pipeline
The ETL (Extract, Transform, Load) pipeline moves data from raw JSON files into the PostgreSQL database.

### 🔄 ETL Strategy
1.  **Process Song Data (`data/song_data`)**
    * Read JSON song metadata files.
    * Insert song records into the `songs` table.
    * Insert artist records into the `artists` table.

2.  **Process Log Data (`data/log_data`)**
    * Filter records where `page = 'NextSong'`.
    * Convert timestamps from milliseconds to datetime.
    * Insert time data into the `time` table.
    * Insert or update user records in the `users` table.
    * **Lookup `song_id` and `artist_id`** using song title, artist name, and duration.
    * Insert records into the `songplays` fact table.

---

## 4. Project Structure

```bash
.
├── data/
│   ├── song_data/
│   └── log_data/
├── sql_queries.py      # SQL create, drop, and insert queries
├── create_tables.py    # Drops and recreates all tables
├── etl.py              # ETL pipeline script
├── verify_data.py      # Optional script to validate data
└── README.md           # Project documentation
