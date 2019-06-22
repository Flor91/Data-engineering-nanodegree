# Project: Data Lake

## Introduction

*A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.*

In this project we will build an ETL pipeline that extracts their data from the data lake hosted on S3, processes them using Spark which will be deployed on an EMR cluster using AWS, and load the data back into S3 as a set of dimensional tables in parquet format. 

From this tables we will be able to find insights in what songs their users are listening to.

## How to run

*To run this project in local mode*, create a file `dl.cfg` in the root of this project with the following data:

```
KEY=YOUR_AWS_ACCESS_KEY
SECRET=YOUR_AWS_SECRET_KEY
```

Create an S3 Bucket named `sparkify-dend` where output results will be stored.

Finally, run the following command:

`python etl.py`

*To run on an Jupyter Notebook powered by an EMR cluster*, import the notebook found in this project.

## Project structure

The files found at this project are the following:

- dl.cfg: *not uploaded to github - you need to create this file yourself* File with AWS credentials.
- etl.py: Program that extracts songs and log data from S3, transforms it using Spark, and loads the dimensional tables created in parquet format back to S3.
- README.md: Current file, contains detailed information about the project.

## ETL pipeline

1. Load credentials
2. Read data from S3
    - Song data: `s3://udacity-dend/song_data`
    - Log data: `s3://udacity-dend/log_data`

    The script reads song_data and load_data from S3.

3. Process data using spark

    Transforms them to create five different tables listed under `Dimension Tables and Fact Table`.
    Each table includes the right columns and data types. Duplicates are addressed where appropriate.

4. Load it back to S3

    Writes them to partitioned parquet files in table directories on S3.

    Each of the five tables are written to parquet files in a separate analytics directory on S3. Each table has its own folder within the directory. Songs table files are partitioned by year and then artist. Time table files are partitioned by year and month. Songplays table files are partitioned by year and month.

### Source Data
- **Song datasets**: all json files are nested in subdirectories under *s3a://udacity-dend/song_data*. A sample of this files is:

```
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```

- **Log datasets**: all json files are nested in subdirectories under *s3a://udacity-dend/log_data*. A sample of a single row of each files is:

```
{"artist":"Slipknot","auth":"Logged In","firstName":"Aiden","gender":"M","itemInSession":0,"lastName":"Ramirez","length":192.57424,"level":"paid","location":"New York-Newark-Jersey City, NY-NJ-PA","method":"PUT","page":"NextSong","registration":1540283578796.0,"sessionId":19,"song":"Opium Of The People (Album Version)","status":200,"ts":1541639510796,"userAgent":"\"Mozilla\/5.0 (Windows NT 6.1) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"","userId":"20"}
```

### Dimension Tables and Fact Table

**songplays** - Fact table - records in log data associated with song plays i.e. records with page NextSong
- songplay_id (INT) PRIMARY KEY: ID of each user song play 
- start_time (DATE) NOT NULL: Timestamp of beggining of user activity
- user_id (INT) NOT NULL: ID of user
- level (TEXT): User level {free | paid}
- song_id (TEXT) NOT NULL: ID of Song played
- artist_id (TEXT) NOT NULL: ID of Artist of the song played
- session_id (INT): ID of the user Session 
- location (TEXT): User location 
- user_agent (TEXT): Agent used by user to access Sparkify platform

**users** - users in the app
- user_id (INT) PRIMARY KEY: ID of user
- first_name (TEXT) NOT NULL: Name of user
- last_name (TEXT) NOT NULL: Last Name of user
- gender (TEXT): Gender of user {M | F}
- level (TEXT): User level {free | paid}

**songs** - songs in music database
- song_id (TEXT) PRIMARY KEY: ID of Song
- title (TEXT) NOT NULL: Title of Song
- artist_id (TEXT) NOT NULL: ID of song Artist
- year (INT): Year of song release
- duration (FLOAT) NOT NULL: Song duration in milliseconds

**artists** - artists in music database
- artist_id (TEXT) PRIMARY KEY: ID of Artist
- name (TEXT) NOT NULL: Name of Artist
- location (TEXT): Name of Artist city
- lattitude (FLOAT): Lattitude location of artist
- longitude (FLOAT): Longitude location of artist

**time** - timestamps of records in songplays broken down into specific units
- start_time (DATE) PRIMARY KEY: Timestamp of row
- hour (INT): Hour associated to start_time
- day (INT): Day associated to start_time
- week (INT): Week of year associated to start_time
- month (INT): Month associated to start_time 
- year (INT): Year associated to start_time
- weekday (TEXT): Name of week day associated to start_time


## Authors

* **Florencia Silvestre** - [Github](https://github.com/Flor91) - [LinkedIn](https://www.linkedin.com/in/florencia-silvestre-2683587b/)