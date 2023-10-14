import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')


# DROP TABLES
    #staging tables
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
    #star schema tables
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"


# CREATE TABLES
 #staging tables
staging_events_table_create= ("""
        CREATE TABLE staging_events(
               artist              text
             , auth                text
             , firstName           text
             , gender              text
             , intemInSession      int
             , lastName            text
             , length              numeric
             , level               text
             , location            text
             , method              text
             , page                text
             , registration        numeric
             , sessionId           int
             , song                text
             , status              int
             , ts                  timestamp
             , userAgent           text
             , userId              int
    )
""")

staging_songs_table_create = ("""
        CREATE TABLE staging_songs(
              num_songs            int          
            , artist_id            varchar
            , artist_latitude      float
            , artist_longitude     float
            , artist_location      text
            , artist_name          text distkey
            , song_id              varchar
            , title                text
            , duration             numeric
            , year                 int
    
    )
""")

 #star schema tables
songplay_table_create = ("""
         CREATE TABLE songplays(
               songplay_id        int         IDENTITY(0,1) PRIMARY KEY
             , start_time         timestamp   NOT NULL sortkey
             , user_id            int         
             , level              varchar     
             , song_id            varchar     
             , artist_id          varchar     
             , session_id         int 
             , location           varchar
             , user_agent         varchar
    )
""")

user_table_create = ("""
        CREATE TABLE users(
              user_id             int         PRIMARY KEY
            , first_name          varchar
            , last_name           varchar
            , gender              varchar
            , level               varchar
    )
""")

song_table_create = ("""
        CREATE TABLE songs(
              song_id             varchar     PRIMARY KEY
            , title               varchar
            , artist_id           varchar
            , year                int
            , duration            numeric
    )
""")

artist_table_create = ("""
         CREATE TABLE artists(
               artist_id          varchar     PRIMARY KEY
             , name               varchar
             , location           varchar
             , latitude           numeric
             , longitude          numeric
    )
""")

time_table_create = ("""
         CREATE TABLE time(
               start_time         timestamp    PRIMARY KEY
             , hour               int
             , day                int
             , week               int
             , month              int
             , year               int
             , weekday            int
    )
""")


# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events from {} 
    credentials 'aws_iam_role={}'
    TIMEFORMAT as 'epochmillisecs'
    region 'us-west-2' 
    format as json {};
    """).format(config.get('S3', 'LOG_DATA')
                , config.get('IAM_ROLE', 'ARN')
                , config.get('S3', 'LOG_JSONPATH')
               )

staging_songs_copy = ("""
        copy staging_songs from {} 
        credentials 'aws_iam_role={}'
        region 'us-west-2' 
        format as json 'auto';
        
""").format(config.get('S3', 'SONG_DATA')
            , config.get('IAM_ROLE', 'ARN')
           )

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
        SELECT  DISTINCT
              se.ts                AS start_time
            , se.userId            AS user_id
            , se.level
            , ss.song_id
            , ss.artist_id
            , se.sessionId         AS session_id
            , se.location
            , se.userAgent         AS user_agent
        
        FROM staging_events se 
        LEFT JOIN staging_songs ss 
            ON ss.artist_name = se.artist
            AND ss.duration = se.length
            AND ss.title = se.song
            
        WHERE se.page = 'NextSong'
""")

user_table_insert = ("""
    INSERT INTO users  (user_id, first_name, last_name, gender, level)
        SELECT DISTINCT 
              userId        AS user_id
            , firstName     AS first_name
            , lastName      AS last_name
            , gender
            , level
        
        FROM staging_events
            WHERE userId is not null
            AND page='NextSong'
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
        SELECT 
            DISTINCT 
              song_id
            , title
            , artist_id
            , year
            , duration
        
        FROM staging_songs
            WHERE song_id is not null
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
        SELECT 
            DISTINCT 
              artist_id
            , artist_name       AS name
            , artist_location   AS location
            , artist_latitude   AS latitude
            , artist_longitude  AS longitude
        
        FROM staging_songs
            WHERE artist_id is not null
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
        SELECT 
            DISTINCT 
              ts                             AS start_time
            , EXTRACT(hour       FROM ts)    AS hour   
            , EXTRACT(day        FROM ts)    AS day      
            , EXTRACT(week       FROM ts)    AS week
            , EXTRACT(month      FROM ts)    AS month
            , EXTRACT(year       FROM ts)    AS year
            , EXTRACT(dayofweek  FROM ts)    AS  weekday
        
        FROM staging_events
            WHERE ts is not null
            AND page='NextSong'
""")

# ANALYTICAL QUERIES
    #row count
get_number_staging_events = ("""SELECT COUNT(*) FROM staging_events""")
get_number_staging_songs = ("""SELECT COUNT(*) FROM staging_songs""")
get_number_songplays = ("""SELECT COUNT(*) FROM songplays""")
get_number_users = ("""SELECT COUNT(*) FROM users""")
get_number_songs = ("""SELECT COUNT(*) FROM songs""")
get_number_artists = ("""SELECT COUNT(*) FROM artists""")
get_number_time = ("""SELECT COUNT(*) FROM time""")

 #analytics query

song_per_session = "SELECT count(distinct songplay_id)/count(distinct session_id) as songs_per_session FROM songplays"

most_played_song = "SELECT s.title, count(*) as num_of_records\
   FROM songplays sp\
   JOIN songs s ON s.song_id = sp.song_id \
   GROUP BY s.title \
   ORDER BY  count(*) desc \
   LIMIT  1"

peak_hour =  "SELECT hour, count(*) as num_of_records \
    FROM songplays sp \
    JOIN time t ON t.start_time = sp.start_time \
    GROUP BY hour \
    ORDER BY num_of_records DESC\
    LIMIT 1"


# QUERY LISTS
create_table_queries = [
      staging_events_table_create
    , staging_songs_table_create
    , songplay_table_create
    , user_table_create
    , song_table_create
    , artist_table_create
    , time_table_create
]

drop_table_queries = [
      staging_events_table_drop
    , staging_songs_table_drop
    , songplay_table_drop
    , user_table_drop
    , song_table_drop
    , artist_table_drop
    , time_table_drop
]

copy_table_queries = [
      staging_events_copy
    , staging_songs_copy
]

insert_table_queries = [
     songplay_table_insert
    , user_table_insert
    , song_table_insert
    , artist_table_insert
    , time_table_insert
]

select_number_rows_queries = [
      get_number_staging_events
    , get_number_staging_songs
    , get_number_songplays
    , get_number_users
    , get_number_songs
    , get_number_artists
    , get_number_time
]

analytics_queries = [
     song_per_session
    , most_played_song
    , peak_hour
]