import configparser



# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

drop_table_queries = "DROP TABLE IF EXISTS "

staging_events_table_drop = drop_table_queries + "staging_events"
staging_songs_table_drop = drop_table_queries + "staging_songs"
songplay_table_drop = drop_table_queries + "songplays"
user_table_drop = drop_table_queries + "users"
song_table_drop = drop_table_queries  + "songs"
artist_table_drop = drop_table_queries + "artists"
time_table_drop = drop_table_queries + "times"

# CREATE TABLES

staging_events_table_create= "CREATE TABLE IF NOT EXISTS staging_events (" \
                             "artist VARCHAR(1000)," \
                             "auth VARCHAR(1000)," \
                             "firstName VARCHAR(1000)," \
                             "gender VARCHAR(1000)," \
                             "itemInSession bigint  ," \
                             "lastName VARCHAR(1000)," \
                             "length FLOAT," \
                             "level VARCHAR(1000)," \
                             "location VARCHAR(1000)," \
                             "method VARCHAR(1000)," \
                             "page VARCHAR(1000)," \
                             "registration FLOAT," \
                             "sessionId bigint ," \
                             "song VARCHAR(1000)," \
                             "status bigint ," \
                             "ts bigint ," \
                             "userAgent VARCHAR(1000)," \
                             "userId VARCHAR(1000))"

staging_songs_table_create = "CREATE TABLE IF NOT EXISTS staging_songs (" \
                             "num_songs bigint  NOT NULL," \
                             "artist_id VARCHAR(1000)," \
                             "artist_latitude FLOAT," \
                             "artist_longitude FLOAT," \
                             "artist_location VARCHAR(1000)," \
                             "artist_name VARCHAR(1000)," \
                             "song_id VARCHAR(1000) NOT NULL," \
                             "title VARCHAR(1000)," \
                             "duration FLOAT," \
                             "year bigint )"

songplay_table_create = "CREATE TABLE IF NOT EXISTS songplays (" \
                             "songplay_id INT IDENTITY(0,1) PRIMARY KEY ," \
                             "start_time bigint  NOT NULL REFERENCES times(start_time)," \
                             "user_id VARCHAR(1000) NOT NULL REFERENCES users(user_id)," \
                             "level VARCHAR(1000)," \
                             "song_id VARCHAR(1000) NOT NULL REFERENCES songs(song_id)," \
                             "artist_id VARCHAR(1000) NOT NULL REFERENCES artists(artist_id)," \
                             "session_id INTEGER NOT NULL," \
                             "location VARCHAR(1000)," \
                             "user_agent VARCHAR(1000))"

user_table_create = """
                    CREATE TABLE IF NOT EXISTS users (
                        user_id VARCHAR(1000)  PRIMARY KEY,
                        first_name VARCHAR(1000),
                        last_name VARCHAR(1000),
                        gender VARCHAR(1000),
                        level VARCHAR(1000)
                    );
                    """

song_table_create = "CREATE TABLE IF NOT EXISTS songs (" \
                             "song_id VARCHAR(1000) NOT NULL PRIMARY KEY," \
                             "title VARCHAR(1000)," \
                             "artist_id VARCHAR(1000) NOT NULL," \
                             "year INTEGER," \
                             "duration FLOAT)"

artist_table_create = "CREATE TABLE IF NOT EXISTS artists (" \
                             "artist_id VARCHAR(1000) PRIMARY KEY," \
                             "name VARCHAR(1000)," \
                             "location VARCHAR(1000)," \
                             "latitude FLOAT," \
                             "longitude FLOAT)"


time_table_create = "CREATE TABLE IF NOT EXISTS times (" \
                             "start_time TIMESTAMP PRIMARY KEY," \
                             "hour INT NOT NULL ," \
                             "day INT NOT NULL ," \
                             "week INT NOT NULL ," \
                             "month INT NOT NULL ," \
                             "year INT NOT NULL ," \
                             "weekday INT NOT NULL)"
# STAGING TABLES
# Data is copied from S3 to staging tables in Redshift. logs are partitioned by year and month.
# example log_data/2018/11/2018-11-12-events.json
staging_events_copy = """
                        COPY staging_events FROM {} 
                        CREDENTIALS 'aws_iam_role={}' 
                        FORMAT AS JSON {} 
                        REGION 'us-west-2';
""".format(config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
                        COPY staging_songs FROM {} 
                        CREDENTIALS 'aws_iam_role={}' 
                        FORMAT AS JSON 'auto'
                        REGION 'us-west-2';
""").format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = "INSERT INTO songplays " \
                        "(start_time, user_id, level, " \
                        "song_id, artist_id,session_id, location, user_agent) " \
                        "SELECT se.ts AS start_time," \
                        "se.userId AS user_id, " \
                        "se.level AS level, " \
                        "ss.song_id AS song_id," \
                        "ss.artist_id AS artist_id," \
                        "se.sessionId AS session_id," \
                        "se.location AS location, " \
                        "se.userAgent AS user_agent " \
                        "FROM staging_events se " \
                        "JOIN staging_songs ss " \
                        "ON ss.title = se.song" \

user_table_insert = "INSERT INTO users (user_id, first_name, last_name, gender, level) " \
                    "SELECT DISTINCT userId AS user_id, " \
                    "firstName as first_name, " \
                    "lastName as last_name," \
                    "gender, " \
                    "level " \
                    "FROM staging_events" \

song_table_insert = "INSERT INTO songs (song_id, title, artist_id, year, duration) " \
                    "SELECT DISTINCT song_id, " \
                    "title," \
                    "artist_id," \
                    "year," \
                    "duration " \
                    "FROM staging_songs"

artist_table_insert = "INSERT INTO artists (artist_id, name, location, latitude, longitude) " \
                      "SELECT DISTINCT artist_id, " \
                      "artist_name, " \
                      "artist_location, " \
                      "artist_latitude, " \
                      "artist_longitude " \
                      "FROM staging_songs" \

time_table_insert = "INSERT INTO times (start_time, hour, day, week, month, year, weekday) " \
                    "SELECT DISTINCT " \
                    "TIMESTAMP 'epoch' + ts * INTERVAL '1 second' AS start_time, " \
                    "EXTRACT(hour FROM TIMESTAMP 'epoch' + ts * INTERVAL '1 second') AS hour, " \
                    "EXTRACT(day FROM TIMESTAMP 'epoch' + ts * INTERVAL '1 second') AS day, " \
                    "EXTRACT(week FROM TIMESTAMP 'epoch' + ts * INTERVAL '1 second') AS week," \
                    "EXTRACT(month FROM TIMESTAMP 'epoch' + ts * INTERVAL '1 second') AS month, " \
                    "EXTRACT(year FROM TIMESTAMP 'epoch' + ts * INTERVAL '1 second') AS year, " \
                    "EXTRACT(DOW FROM TIMESTAMP 'epoch' + ts * INTERVAL '1 second') AS weekday " \
                    "FROM staging_events"

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create,
                        artist_table_create, time_table_create, songplay_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop,
                      songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]
# staging_events_copy, staging_songs_copy
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert,
                        time_table_insert, songplay_table_insert]

#