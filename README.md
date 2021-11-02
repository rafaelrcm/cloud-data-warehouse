# Description: 
In this project I will extract data from s3 to redshift, populate staging tables and from these tables populate the dimension and fact tables.

# S3:
log_data: contains a folder with json files, will be used to populate the staging_events table.
song_data: contains a folder with json files, will be used to populate the staging_songs table.
log_json_path.json: json file, will be used in copy command of song_data.

# Staging Tables:
staging_events: staging_event_id, artist, auth, first_name, gender, item_in_session, last_name, length, level, location, method, page, registration, session_id, song, status, ts, user_agent, user_id

staging_songs: artist_id, artist_latitude, artist_location, artist_longitude, artist_name, duration, num_songs, song_id, title, year

# Fact Table:
songplays: serial, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

# Dim Tables:
users: user_id, first_name, last_name, gender, level
songs: song_id, title, artist_id, year, duration
artists: artist_id, name, location, latitude, longitude
time: start_time, hour, day, week, month, year, weekday

# Special Cases:
To handle duplicate scenarios the clause DISTINCT is applied to SQL Queries

# AWS Configuration:
- Set the redshift configuration in dwh.cfg

# Run the scripts:
- Run the create_tables.py
- Run the etl.py
