import sqlite3
from sqlite3 import Error
import time


class database:
    connection = None

    songs_table = """ CREATE TABLE IF NOT EXISTS songs (
                                            song_id text NOT NULL,
                                            audition_id text NOT NULL,
                                            artist text NOT NULL,
                                            title text NOT NULL); """

    auditions_table = """CREATE TABLE IF NOT EXISTS auditions (
                                        user_id text NOT NULL,
                                        song_id text NOT NULL,
                                        audition_date text NOT NULL);"""

    def __init__(self, db_file):
        try:
            self.connection = sqlite3.connect(db_file)
            if self.connection is not None:
                print("connected")
                self.create_table(self.songs_table)
                self.create_table(self.auditions_table)
                print("tables created")
            else:
                print("connection error: cannot create database tables")
        except Error as e:
            print(e)

    def create_table(self, create_table_sql):
        try:
            c = self.connection.cursor()
            c.execute(create_table_sql)
        except Error as e:
            print(e)

    def insert(self, insert_table_sql):
        try:
            c = self.connection.cursor()
            c.execute(insert_table_sql)
            return c.lastrowid
        except Error as e:
            print(e)

    def select(self, select_sql):
        try:
            c = self.connection.cursor()
            c.execute(select_sql)
            rows = c.fetchall()
            for row in rows:
                print(row)
        except Error as e:
            print(e)



class file_operations:

    database_operations = None

    best_artist_sql = """ SELECT artist FROM auditions a 
                          JOIN songs s ON a.song_id = s.audition_id 
                          GROUP BY s.artist 
                          ORDER BY count(*) 
                          DESC LIMIT 1 """

    top_tracks_sql = """ SELECT artist, title, count(*) FROM auditions a
                         JOIN songs s ON a.song_id = s.audition_id 
                         GROUP BY s.artist, s.title 
                         ORDER BY count(*) 
                         DESC LIMIT 5 """

    def __init__(self, database):
        self.database_operations = database

    def auditions_file_insert (self, file):
        start_time = time.time()
        with open(file, encoding="utf8", errors='ignore') as infile:
            for row in infile:
                row = row.replace("'", "")
                data = row.split('<SEP>')
                query = 'INSERT INTO auditions(user_id, song_id, audition_date) VALUES (\'' + data[0] \
                        + '\',\'' + data[1] + '\',\'' + data[2] + '\')'

                self.database_operations.insert(query)

        self.database_operations.connection.commit()
        print(file + " added to database in time: %s seconds" % (time.time() - start_time))

    def songs_file_insert(self, file):
        start_time = time.time()
        with open(file, encoding="utf8", errors='ignore') as infile:
            for row in infile:
                row = row.replace("'", "")
                data = row.split('<SEP>')
                query = 'INSERT INTO songs(song_id, audition_id, artist, title) VALUES (\'' + data[0] \
                        + '\',\'' + data[1] + '\',\'' + data[2] + '\',\'' + data[3] + '\')'

                self.database_operations.insert(query)

        self.database_operations.connection.commit()
        print(file + " added to database in time: %s seconds" % (time.time() - start_time))

    def best_artist(self):
        start_time = time.time()

        self.database_operations.select(self.best_artist_sql)
        print("Best artist search process done in time: %s seconds" % (time.time() - start_time))

    def top_tracks(self):
        start_time = time.time()

        self.database_operations.select(self.top_tracks_sql)
        print("Best artist search process done in time: %s seconds" % (time.time() - start_time))






file_name = "songs-database2.db"
db = database(file_name)

start_time = time.time()
files = file_operations(db)

print(" ")
print("Starting files adding process ")

files.songs_file_insert("unique_tracks.txt")
files.auditions_file_insert("triplets_sample_20p.txt")

print(" ")
print("Starting search process ")
print(" ")
print("***** best artist ******")
files.best_artist()
print(" ")
print(" ***** TOP 5 tracks ***** ")
files.top_tracks()
print(" ")
print(" ")
print("Whole process took: %s seconds" % (time.time() - start_time))



