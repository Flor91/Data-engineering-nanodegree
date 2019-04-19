import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """Reads songs log file row by row, selects needed fields and inserts them into song and artist tables.

        Parameters:
            cur (psycopg2.cursor()): Cursor of the sparkifydb database
            filepath (str): Filepath of the file to be analyzed
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    for value in df.values:
        artist_id, artist_latitude, artist_location, artist_longitude, artist_name, duration, num_songs, song_id, title, year = value

        # insert artist record
        artist_data = [artist_id, artist_name, artist_location, artist_longitude, artist_latitude]
        cur.execute(artist_table_insert, artist_data)

        # insert song record
        song_data = [song_id, title, artist_id, year, duration]
        cur.execute(song_table_insert, song_data)


def process_log_file(cur, filepath):
    """Reads user activity log file row by row, filters by NexSong, selects needed fields, transforms them and inserts
    them into time, user and songplay tables.

            Parameters:
                cur (psycopg2.cursor()): Cursor of the sparkifydb database
                filepath (str): Filepath of the file to be analyzed
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page']=='NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms') 
    
    # insert time data records
    time_data = []
    for line in t:
        time_data.append([line, line.hour, line.day, line.week, line.month, line.year, line.day_name()])
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame.from_records(time_data, columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

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

        # insert songplay record
        songplay_data = (index, pd.to_datetime(row.ts, unit='ms'), int(row.userId), row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """Walks through all files nested under filepath, and processes all logs found.

    Parameters:
        cur (psycopg2.cursor()): Cursor of the sparkifydb database
        conn (psycopg2.connect()): Connectio to the sparkifycdb database
        filepath (str): Filepath parent of the logs to be analyzed
        func (python function): Function to be used to process each log

    Returns:
        Name of files processed
    """
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
    
    return all_files


def main():
    """Function used to extract, transform all data from song and user activity logs and load it into a PostgreSQL DB
        Usage: python etl.py
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()