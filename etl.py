import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    - Loads the song file
    - Extracts the appropriate columns from the file
    - Inserts records into song table and artists table
    """
    # open song file
    df = pd.read_json(filepath,lines=True)

    # insert song record
    df1 = df[['song_id', 'title', 'artist_id', 'year', 'duration']]
    song_data = list(df1.values[0])
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']]
    artist_data = list(artist_data.values[0])
    cur.execute(artist_table_insert, artist_data)

def process_log_file(cur, filepath):
    """
    - Loads the log file 
    - Handles transformation, datatype of various columns
    - Inserts data into users and time table
    - Gets song_id and artist_id using log file information
    - Inserts record into songplays table by using log file and song file information
    """
    # open log file
    df = pd.read_json(filepath,lines=True)

    # filter by NextSong action
    df = df[df.page=="NextSong"]

    # convert timestamp column to datetime
    df['ts'] = pd.to_datetime(df.ts,unit='ms')
    
    # insert time data records
    df['ts'] = pd.to_datetime(df.ts,unit='ms')
    df['hour'] = df['ts'].dt.hour
    df['day'] = df['ts'].dt.day
    df['week'] = df['ts'].dt.weekofyear
    df['month'] = df['ts'].dt.month
    df['year'] = df['ts'].dt.year
    df['weekday'] = df['ts'].dt.weekday

    x = df[['ts','hour','day','week','month','year','weekday']].values.tolist()
    column_labels = ['art_time','hour','day','week','month','year','weekday']
    time_df = pd.DataFrame(x,columns= column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    df = pd.read_json(filepath,lines=True)
    df = df[df.userId.apply(lambda x: str(x).isdigit())]
    df['userId'] = df.userId.astype('int')
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    user_df = user_df.drop_duplicates(subset='userId', keep="first")

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    df['ts'] = pd.to_datetime(df.ts,unit='ms')
    for index, row in df.iterrows():
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results

        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid,artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Processes the data for all the files in the directory
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


def main():
    """
    - Connect to Sparkifydb 
    - Call process_data function to process all the input files
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()