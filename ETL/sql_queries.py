import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop  = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop       = "DROP TABLE IF EXISTS songplays;"
user_table_drop           = "DROP TABLE IF EXISTS users;"
song_table_drop           = "DROP TABLE IF EXISTS songs;"
artist_table_drop         = "DROP TABLE IF EXISTS artists;"
time_table_drop           = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (artist_name    VARCHAR,
                                           auth           VARCHAR,
                                           firstName      VARCHAR,
                                           gender         VARCHAR,
                                           itemInSession  INTEGER,
                                           lastName       VARCHAR,
                                           length         DECIMAL,
                                           level          VARCHAR,
                                           location       VARCHAR,
                                           method         VARCHAR,
                                           page           VARCHAR,
                                           registration   DECIMAL,
                                           sessionId      INTEGER,
                                           song           VARCHAR,
                                           status         INTEGER,
                                           ts             TIMESTAMP,
                                           userAgent      VARCHAR,
                                           userId         INTEGER);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (artist_id         VARCHAR,
                                          artist_latitude   DECIMAL,
                                          artist_location   VARCHAR,
                                          artist_longitude  DECIMAL,
                                          artist_name       VARCHAR,
                                          num_songs         INTEGER,
                                          song_id           VARCHAR,
                                          title             VARCHAR,
                                          duration          DECIMAL,
                                          year              INTEGER);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (songplay_id    BIGINT IDENTITY(0,1) PRIMARY KEY NOT NULL sortkey,
                                      start_time     TIMESTAMP NOT NULL,
                                      user_id        INTEGER NOT NULL,
                                      level          VARCHAR NOT NULL,
                                      song_id        VARCHAR NOT NULL distkey,
                                      artist_id      VARCHAR NOT NULL,
                                      session_id     INTEGER NOT NULL,
                                      location       VARCHAR NOT NULL,
                                      user_agent     VARCHAR NOT NULL);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (user_id       INTEGER PRIMARY KEY NOT NULL sortkey,
                                  first_name    VARCHAR NOT NULL,
                                  last_name     VARCHAR NOT NULL,
                                  gender        VARCHAR NOT NULL,
                                  level         VARCHAR NOT NULL) diststyle all;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (song_id     VARCHAR PRIMARY KEY NOT NULL sortkey distkey,
                                  title       VARCHAR NOT NULL,
                                  artist_id   VARCHAR NOT NULL,
                                  year        INTEGER NOT NULL,
                                  duration    DECIMAL NOT NULL);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (artist_id   VARCHAR PRIMARY KEY NOT NULL sortkey,
                                    artist_name VARCHAR NOT NULL,
                                    location    VARCHAR,
                                    latitude   DECIMAL,
                                    longitude   DECIMAL) diststyle all;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (start_time TIMESTAMP PRIMARY KEY NOT NULL sortkey,
                                 hour       INTEGER NOT NULL,
                                 day        INTEGER NOT NULL,
                                 week       INTEGER NOT NULL,
                                 month      INTEGER NOT NULL,
                                 year       INTEGER NOT NULL,
                                 weekday    INTEGER NOT NULL) diststyle all;
""")


# STAGING TABLES
staging_events_copy = ("""copy staging_events from {} 
                          credentials 'aws_iam_role={}'
                          region 'us-west-2' FORMAT AS JSON{} TIMEFORMAT as 'epochmillisecs';
                       """.format(config.get("S3","LOG_DATA"), config.get("IAM_ROLE","ARN"),config.get("S3","LOG_JSONPATH")))

staging_songs_copy = ("""copy staging_songs from {} 
                         credentials 'aws_iam_role={}'
                         region 'us-west-2' JSON 'auto';
                      """.format(config.get("S3","SONG_DATA"), config.get("IAM_ROLE","ARN")))


# FINAL TABLES
songplay_table_insert = ("""INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                            SELECT DISTINCT staging_events.ts, staging_events.userId, staging_events.level, staging_songs.song_id,
                                            staging_songs.artist_id, staging_events.sessionId, staging_events.location, 
                                            staging_events.userAgent
                            FROM staging_events
                            JOIN staging_songs ON (staging_events.artist_name = staging_songs.artist_name);
                         """)

user_table_insert = ("""INSERT INTO users(user_id, first_name, last_name, gender, level)
                        SELECT DISTINCT userId, firstName, lastName, gender, level
                        FROM staging_events
                        WHERE userId IS NOT NULL;
                     """)

song_table_insert = ("""INSERT INTO songs(song_id, title, artist_id, year, duration)
                        SELECT DISTINCT song_id, title, artist_id, year, duration
                        FROM staging_songs;
                     """)

artist_table_insert = ("""INSERT INTO artists(artist_id, artist_name, location, latitude, longitude)
                          SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
                          FROM staging_songs;
                       """)

time_table_insert = ("""INSERT INTO time(start_time, hour, day, week, month, year, weekday)
                        SELECT ts, EXTRACT(hour FROM ts) as hour, EXTRACT(day FROM ts) as day, EXTRACT(week FROM ts) as week,
                        EXTRACT(month FROM ts) as month, EXTRACT(year FROM ts) as year, EXTRACT(weekday FROM ts) as weekday
                        FROM staging_events; 
                     """)


# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
