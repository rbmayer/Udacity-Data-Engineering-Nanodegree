import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
LOG_DATA = config.get('S3', 'LOG_DATA')
SONG_DATA = config.get('S3', 'SONG_DATA')
ARN = config.get('IAM_ROLE', 'ARN')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
REGION = config.get('GEO', 'REGION')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events (
    artist text, 
    auth text, 
    firstName text, 
    gender text, 
    ItemInSession int, 
    lastName text, 
    length float8, 
    level text, 
    location text, 
    method text, 
    page text, 
    registration text, 
    sessionId int, 
    song text, 
    status int, 
    ts timestamp, 
    userAgent text, 
    userId int)
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (
    song_id text PRIMARY KEY, 
    artist_id text, 
    artist_latitude float8, 
    artist_location text, 
    artist_longitude float8, 
    artist_name text, 
    duration float8, 
    num_songs int, 
    title text, 
    year int)
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
    songplay_id int IDENTITY PRIMARY KEY, 
    start_time timestamp NOT NULL REFERENCES time(start_time) sortkey, 
    user_id int NOT NULL REFERENCES users(user_id), 
    level text NOT NULL, 
    song_id text NOT NULL REFERENCES songs(song_id), 
    artist_id text NOT NULL REFERENCES artists(artist_id) distkey, 
    session_id int NOT NULL, 
    location text NOT NULL, 
    user_agent text NOT NULL)
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
    user_id int PRIMARY KEY sortkey, 
    first_name text NOT NULL, 
    last_name text NOT NULL, 
    gender text NOT NULL, 
    level text NOT NULL)
    diststyle ALL;
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
    song_id text PRIMARY KEY sortkey, 
    title text NOT NULL, 
    artist_id text NOT NULL, 
    year int NOT NULL, 
    duration numeric NOT NULL)
    diststyle ALL;
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
    artist_id text PRIMARY KEY distkey, 
    name text NOT NULL, 
    location text NOT NULL, 
    lattitude float8 NOT NULL, 
    longitude float8 NOT NULL)
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
    start_time timestamp PRIMARY KEY sortkey, 
    hour int NOT NULL, 
    day int NOT NULL, 
    week int NOT NULL, 
    month int NOT NULL, 
    year int NOT NULL, 
    weekday int NOT NULL)
""")

# STAGING TABLES

staging_events_copy = ("""COPY staging_events FROM {} iam_role {} region {} FORMAT AS JSON {} timeformat 'epochmillisecs';
""").format(LOG_DATA, ARN, REGION, LOG_JSONPATH)

staging_songs_copy = ("""COPY staging_songs FROM {} iam_role {} region {} FORMAT AS JSON 'auto';
""").format(SONG_DATA, ARN, REGION)

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
                            SELECT DISTINCT se.ts, se.userId, se.level, ss.song_id, ss.artist_id, se.sessionId, se.location, se.userAgent
                            FROM staging_events se 
                            INNER JOIN staging_songs ss 
                                ON se.song = ss.title
                            WHERE se.page = 'NextSong'
""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
                        SELECT DISTINCT se.userId, se.firstName, se.lastName, se.gender, se.level
                        FROM staging_events se
                        WHERE se.userId IS NOT NULL
""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration) 
                        SELECT DISTINCT ss.song_id, ss.title, ss.artist_id, ss.year, ss.duration
                        FROM staging_songs ss
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, lattitude, longitude)
                          SELECT DISTINCT ss.artist_id, 
                                          ss.artist_name, 
                                          CASE WHEN ss.artist_location IS NULL THEN 'N/A' ELSE ss.artist_location END, 
                                          CASE WHEN ss.artist_latitude IS NULL THEN 0.0 ELSE ss.artist_latitude END, 
                                          CASE WHEN ss.artist_longitude IS NULL THEN 0.0 ELSE ss.artist_longitude END
                          FROM staging_songs ss
                          WHERE ss.artist_id IS NOT NULL
""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
                        SELECT DISTINCT 
                               se.ts, 
                               CAST(DATE_PART('hour', se.ts) as Integer), 
                               CAST(DATE_PART('day', se.ts) as Integer), 
                               CAST(DATE_PART('week', se.ts) as Integer),
                               CAST(DATE_PART('month', se.ts) as Integer),
                               CAST(DATE_PART('year', se.ts) as Integer),
                               CAST(DATE_PART('dow', se.ts) as Integer)
                        FROM staging_events se
                        WHERE se.page = 'NextSong'
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
