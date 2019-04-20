import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """Insert record from JSON song file into postgresql tables.
    
    Read JSON file to pandas dataframe, clean and process data, 
    then load to song and artist tables.
    
    Parameters:
    cur (cursor object): connection cursor
    filepath (string): filepath
    
    Returns: None
    
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df.loc[(df['song_id'].notnull() & df['title'].notnull() 
                        & df['artist_id'].notnull() & df['year'].notnull()), 
                       ['song_id', 'title', 'artist_id', 'year', 'duration']].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df.loc[(df['artist_id'].notnull() & df['artist_name'].notnull()), 
                         ['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 
                          'artist_longitude']].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """Insert records from JSON log files into PostgreSQL tables.
    
    Read JSON file to pandas dataframe, clean and process data, 
    then load to user and songplay tables.
    
    Parameters:
    cur (cursor object): connection cursor
    filepath (string): filepath
    
    Returns: None
    
    """
    # open log file
    df = pd.read_json(filepath, lines=True) 

    # filter by NextSong action
    df = df.loc[df.page=='NextSong']

    # convert timestamp column to datetime
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert non-null time data records
    time_data = list((t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday))
    column_labels = ('timestamp', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame.from_dict(dict(zip(column_labels,time_data)))
    time_df = time_df.loc[time_df['timestamp'].notnull()]

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    # filter out rows with no user id, gender, level or timestamp
    user_df = df.loc[(df['userId'].notnull() & df['gender'].notnull() 
                      & df['level'].notnull() & df['ts'].notnull()), 
                     ['userId', 'firstName', 'lastName', 'gender', 'level', 'ts']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None
            
        # create songplay uuid
        name = (str(row.song) + str(row.ts) + str(row.userId),)
        generate_uuid = ("""SELECT uuid_generate_v5(uuid_nil(), %s)""")
        cur.execute(generate_uuid, name)
        songplayid = cur.fetchone() 

        # insert songplay record
        songplay_data = (songplayid, row.ts, row.userId, row.level, songid, artistid, 
                         row.sessionId, row.location, row.userAgent)
        if row.ts is not None:
            cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """Process data files from directory using function."""
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
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
    """Load song and log data into postgresql star schema."""
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    
    # enable uuid extension
    cur.execute("""CREATE EXTENSION "uuid-ossp";""")

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()