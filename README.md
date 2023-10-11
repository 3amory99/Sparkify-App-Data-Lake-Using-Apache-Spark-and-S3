# Sparkify-App-Data-Lake-Using-Apache-Spark-and-S3



## Quick start

First, rename dl_template.cfg to dl.cfg and fill in the open fields. Fill in AWS acces key (KEY) and secret (SECRET).

Example data is in data folder. To run the script to use that data, do the wfollowing:

* Create an AWS S3 bucket.
* Edit dl.cfg: add your S3 bucket name.
* Copy **log_data** and **song_data** folders to your own S3 bucket.
* Create **output_data** folder in your S3 bucket.
* NOTE: You can also run script locally and use local input files. Just comment/uncomment rows in `etl.py` main() function.

After installing python3 + Apache Spark (pyspark) libraries and dependencies, run from command line:

* `python3 etl.py` (to process all the input JSON data into Spark parquet files.)

This Project-4 handles data of a music streaming startup, Sparkify. Data set is a set of files in JSON format stored in AWS S3 buckets and contains two parts:

* **s3://udacity-dend/song_data**: static data about artists and songs
  Song-data example:
  `{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}`

* **s3://udacity-dend/log_data**: event data of service usage e.g. who listened what song, when, where, and with which client
  ![Log-data example (log-data/2018/11/2018-11-12-events.json)](./Udacity-DEND-Project4-logdata-20190609.png)

Below, some figures about the example data set (results after running the etl.py):


### Fact Table

* **songplays**: song play data together with user, artist, and song info (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)

### Dimension Tables

* **users**: user info (columns: user_id, first_name, last_name, gender, level)
* **songs**: song info (columns: song_id, title, artist_id, year, duration)
* **artists**: artist info (columns: artist_id, name, location, latitude, longitude)
* **time**: detailed time info about song plays (columns: start_time, hour, day, week, month, year, weekday)

Findings:

* Output data was written to own AWS S3 bucket.
* Both Fact table (songplays) and Dimension tables (users, songs, artists, time) are extracted from inout data and written back to S3 as Spark parquet files.
* Each time `etl.py` is run, it creates new directories for each table to output_data/ directory based on the script start time (e.g. songs_table.parquet_2019-06-10-10-07-20-873438). This way there's no collision or need to erase old data between different runs.
* Spark writes temporary folders and files during the parquet file creation process. Used AWS S3 user requires admin rights for Spark to be able to rename (copy + paste + delete) created temp folders and files.
* In overall, network speed, processing speed, data read & write speed depends heavily on used (AWS) cloud resources. Faster CPU, more RAM, faster disks, faster network connection obviously give better start-to-end time for whole ETL pipeline.
* ETL script requires some optimisation for bigger data amounts (e.g. write parquet files locally and only then upload them to S3).

---

## HOWTO use

**Project has one script:**

* **etl.py**: This script uses data in s3:/udacity-dend/song_data and s3:/udacity-dend/log_data, processes it, and inserts the processed data into DB.

### Prerequisites

Python3 is recommended as the environment. The most convenient way to install python is to use Anaconda (https://www.anaconda.com/distribution/) either via GUI or command line.
Also, the following libraries are needed for the python environment to make Jupyter Notebook and Apache Spark to work:

* _pyspark_ (+ dependencies) to enable script to create a SparkSession. (See https://spark.apache.org/docs/latest/api/python/pyspark.sql.html)
* NOTE: in the beginning of the execution, script downloads hadoop-aws package to enable connection to AWS.

### Run etl.py

Type to command line:

`python3 etl.py`

* Script executes Apache Spark SQL commands to read source data (JSON files) from S3 to memory as Spark DataFrames.
* In memory, data is further manipulated to analytics DataFrames.
* Analytics dataFrames are stored back to S4 as Spark parquet files.
* Script writes to console the query it's executing at any given time and if the query was successfully executed.
* Also, script writes to console DataFrame schemas and show a handful of example data.
* In the end, script tells if whole ETL-pipeline was successfully executed.

Output: input JSON data is processed and analysed data is written back to S3 as Spark parquet files.

## Data cleaning process

`etl.py` works the following way to process the data from source files to analytics tables:

* Loading part of the script (COPY from JSON to staging tables) query takes the data as it is.
* When inserting data from staging tables to analytics tables, queries remove any duplicates (INSERT ... SELECT DISTINCT ...).

## Example queries

* NOTE: There are some example queries implemented in `etl.py` and executed in the end of the script run.
* Get users and songs they listened at particular time. Limit query to 1000 hits:

```
SELECT  sp.songplay_id,
        u.user_id,
        s.song_id,
        u.last_name,
        sp.start_time,
        a.name,
        s.title
FROM songplays AS sp
        JOIN users   AS u ON (u.user_id = sp.user_id)
        JOIN songs   AS s ON (s.song_id = sp.song_id)
        JOIN artists AS a ON (a.artist_id = sp.artist_id)
        JOIN time    AS t ON (t.start_time = sp.start_time)
ORDER BY (sp.start_time)
LIMIT 1000;
```

* Get count of rows in each Dimension table:

```
SELECT COUNT(*)
FROM songs_table;

SELECT COUNT(*)
FROM artists_table;

SELECT COUNT(*)
FROM users_table;

SELECT COUNT(*)
FROM time_table;
```

* Get count of rows in Fact table:

```
SELECT COUNT(*)
FROM songplays_table;
```













### Log Data

The JSON logs on user activity have the following structure.

![log data](https://github.com/Abdelrhman2022/Sparkify-Dend-Data-Lake-with-Apache-Spark/blob/main/Images/log-data.png)






## Data

The data for this project is available on Amazon S3.

- Song data: s3://udacity-dend/song_data
- Log data: s3://udacity-dend/log_data

Sample data is available in the `data/` folder.
