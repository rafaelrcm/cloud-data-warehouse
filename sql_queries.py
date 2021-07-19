import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
staging_event_id int identity(0, 1),
artist varchar,
auth varchar,
first_name varchar,
gender varchar,
item_in_session varchar,
last_name varchar,
length varchar,
level varchar,
location varchar,
method varchar,
page varchar,
registration varchar,
session_id varchar,
song varchar,
status varchar,
ts varchar,
user_agent varchar,
user_id varchar);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
staging_song_id int identity(0, 1),
artist_id varchar,
artist_latitude varchar,
artist_location varchar,
artist_longitude varchar,
artist_name varchar,
duration varchar,
num_songs varchar,
song_id varchar,
title varchar,
year varchar);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
songplay_id int identity(0, 1) NOT NULL PRIMARY KEY, 
start_time timestamp NOT NULL, 
user_id numeric NOT NULL, 
level varchar, 
song_id varchar NOT NULL, 
artist_id varchar NOT NULL, 
session_id numeric, 
location varchar, 
user_agent varchar);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
user_id varchar PRIMARY KEY, 
first_name varchar, 
last_name varchar, 
gender varchar, 
level varchar);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
song_id varchar PRIMARY KEY, 
title varchar, 
artist_id varchar, 
year numeric, 
duration numeric);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
artist_id varchar PRIMARY KEY, 
name varchar, 
location varchar, 
latitude numeric, 
longitude numeric);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
start_time varchar PRIMARY KEY, 
hour numeric, 
day numeric, 
week numeric, 
month numeric, 
year numeric, 
weekday numeric);
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events
from {} 
iam_role {}
JSON {}
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
copy staging_songs
from {}
iam_role {}
format as json 'auto'
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
SELECT DISTINCT
TIMESTAMP 'epoch' + e.ts/1000 * interval '1 second', 
e.user_id, 
e.level, 
s.song_id, 
s.artist_id, 
e.session_id, 
e.location, 
e.user_agent
FROM staging_events e
INNER JOIN staging_songs s
ON s.title=e.song and s.artist_name=e.artist and s.duration=e.length
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level) 
SELECT DISTINCT
e.user_id,
e.first_name,
e.last_name,
e.gender,
e.level 
FROM staging_events e;
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT
s.song_id, 
s.title, 
s.artist_id, 
s.year, 
s.duration
FROM staging_songs s;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude) 
SELECT DISTINCT
s.artist_id, 
s.artist_name, 
s.artist_location, 
s.artist_latitude, 
s.artist_longitude
FROM staging_songs s;
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday) 
SELECT DISTINCT
e.ts,
EXTRACT(hour FROM TIMESTAMP 'epoch' + e.ts/1000 * interval '1 second'),
EXTRACT(day FROM TIMESTAMP 'epoch' + e.ts/1000 * interval '1 second'),
EXTRACT(week FROM TIMESTAMP 'epoch' + e.ts/1000 * interval '1 second'),
EXTRACT(month FROM TIMESTAMP 'epoch' + e.ts/1000 * interval '1 second'),
EXTRACT(year FROM TIMESTAMP 'epoch' + e.ts/1000 * interval '1 second'),
EXTRACT(weekday FROM TIMESTAMP 'epoch' + e.ts/1000 * interval '1 second')
FROM staging_events e;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
