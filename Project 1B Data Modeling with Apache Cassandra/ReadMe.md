# DEND Project 2: Data Modeling with Apache Cassandra

## Project Summary

The objective of this project is to create a NoSQL analytics database in Apache Cassandra for a fictional music streaming service called Sparkify. Sparkify's analytics team seeks to understand what, when and how users are playing songs on the company's music app. The analysts need an easy way to query and analyze the songplay data, which is currently stored in raw csv files on a local directory.

As the data engineer assigned to the project, I have implemented an ETL pipeline in python to pre-process the data using pandas. The database tables are modeled on the queries according to the principle of one table per query. I selected the primary and clustering keys for each table in order to ensure a unique identifier for each row.  

Data Modeling with Apache Cassandra was submitted for Udacity's Data Engineering Nanodegree (DEND) in Spring 2019.

# Part I. ETL Pipeline for Pre-Processing the Files

## PLEASE RUN THE FOLLOWING CODE FOR PRE-PROCESSING THE FILES

#### Import Python packages


```python
# Import Python packages
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv
```

#### Creating list of filepaths to process original event csv data files


```python
# checking your current working directory
print(os.getcwd())

# Get your current folder and subfolder event data
filepath = os.getcwd() + '/event_data'

# Create a for loop to create a list of files and collect each filepath
for root, dirs, files in os.walk(filepath):

# join the file path and roots with the subdirectories using glob
    file_path_list = glob.glob(os.path.join(root,'*'))
    #print(file_path_list)
```

    /home/workspace


#### Processing the files to create the data file csv that will be used for Apache Casssandra tables


```python
# initiating an empty list of rows that will be generated from each file
full_data_rows_list = []

# for every filepath in the file path list
for f in file_path_list:

# reading csv file
    with open(f, 'r', encoding = 'utf8', newline='') as csvfile:
        # creating a csv reader object
        csvreader = csv.reader(csvfile)
        next(csvreader)

 # extracting each data row one by one and append it        
        for line in csvreader:
            #print(line)
            full_data_rows_list.append(line)

# creating a smaller event data csv file called event_datafile_full csv that will be used to insert data into the \
# Apache Cassandra tables
csv.register_dialect('myDialect', quoting=csv.QUOTE_MINIMAL, skipinitialspace=True)

with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
    writer = csv.writer(f, dialect='myDialect')
    writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',\
                'level','location','sessionId','song','userId'])
    for row in full_data_rows_list:
        if (row[0] == ''):
            continue
        writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))

```


```python
# check the number of rows in your csv file
with open('event_datafile_new.csv', 'r', encoding = 'utf8') as f:
    print(sum(1 for line in f))
```

    6821


#### Part I code was provided by course instructors with limited tweaks by the student.

# Part II. Modeling Data for Query Retrieval in Apache Cassandra

## This section works with the CSV file titled <font color=red>event_datafile_new.csv</font>, located within the Workspace directory.  The event_datafile_new.csv contains the following columns:
- artist
- firstName of user
- gender of user
- item number in session
- last name of user
- length of the song
- level (paid or free song)
- location of the user
- sessionId
- song title
- userId

The image below is a screenshot of the denormalized in <font color=red>**event_datafile_new.csv**</font> after the code above is run:<br>

<img src="images/image_event_datafile_new.jpg">

#### Create Cluster


```python
from cassandra.cluster import Cluster
cluster = Cluster()

session = cluster.connect()
```

#### Create Keyspace


```python
try:
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS sparkify
    WITH REPLICATION =
    { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
)

except Exception as e:
    print(e)
```

#### Set Keyspace


```python
try:
    session.set_keyspace('sparkify')
except Exception as e:
    print(e)
```

## Create tables to run the following queries. Remember, with Apache Cassandra you model the database tables on the queries you want to run.

### Query 1: Give me the artist, song title and song's length in the music app history that was heard during  sessionId = 338, and itemInSession  = 4

#### Create table
The Primary Key for the **songs** table is session_id as the partition key and item_in_session as the clustering key. This will enable fast reads of the table to retrieve song data from a particular session. Partition by session_id ensures that a given playlist history is stored by session id and clustering by item_in_session ensures that the data is sorted by order of play.  


```python
query1 = """CREATE TABLE IF NOT EXISTS songs (
            session_id int, item_in_session int, artist text, song text, length double,
            PRIMARY KEY (session_id, item_in_session)
            )"""

try:
    session.execute(query1)
except Exception as e:
    print(e)
```

#### Insert data


```python
# We have provided part of the code to set up the CSV file. Please complete the Apache Cassandra code below#
file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
## Assign the INSERT statements into the `query` variable
        query = "INSERT INTO songs (session_id, item_in_session, artist, song, length)"
        query = query + " VALUES (%s, %s, %s, %s, %s)"
        ## Assign which column element should be assigned for each column in the INSERT statement.
        ## For e.g., to INSERT artist_name and user first_name, you would change the code below to `line[0], line[1]`
        session.execute(query, (int(line[8]), int(line[3]), line[0], line[9], float(line[5])))
```

#### Run SELECT query to verify table model


```python
query1 = """SELECT artist, song, length FROM songs WHERE session_id = 338 AND item_in_session = 4"""

try:
    rows = session.execute(query1)
except Exception as e:
    print(e)

for row in rows:
    print( row.artist, row.song, row.length)
```

    Faithless Music Matters (Mark Knight Dub) 495.3073


### Query 2:  Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182

#### Create table


```python
query2 = """CREATE TABLE IF NOT EXISTS user_sessions (
            user_id int, session_id int, item_in_session int, artist text, song text, first_name text, last_name text,
            PRIMARY KEY ((user_id, session_id), item_in_session)
            )"""

try:
    session.execute(query2)
except Exception as e:
    print(e)
```

#### Insert data


```python
file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
        query = "INSERT INTO user_sessions (user_id, session_id, item_in_session, artist, song, first_name, last_name)"
        query = query + " VALUES (%s, %s, %s, %s, %s, %s, %s)"
        session.execute(query, (int(line[10]), int(line[8]), int(line[3]), line[0], line[9], line[1], line[4]))
```

#### Run SELECT query to verify table model


```python
query2 = """SELECT artist, song, first_name, last_name FROM user_sessions WHERE user_id = 10 AND session_id = 182"""

try:
    rows = session.execute(query2)
except Exception as e:
    print(e)

for row in rows:
    print( row.artist, row.song, row.first_name, row.last_name )
```

    Down To The Bone Keep On Keepin' On Sylvie Cruz
    Three Drives Greece 2000 Sylvie Cruz
    Sebastien Tellier Kilometer Sylvie Cruz
    Lonnie Gordon Catch You Baby (Steve Pitron & Max Sanna Radio Edit) Sylvie Cruz


### Query 3: Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'

#### Create table

Since the query specifies the data to retrieve by song, I have used song as the partition key. Song name, alone, is not sufficient to define a unique record. A possible choice for clustering would be first name and/or last name. I chose user_id as the clustering column because it is unique per user, whereas many people may share the same name.


```python
## Query 3: Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
query3 = """CREATE TABLE IF NOT EXISTS song_users (
            song text, user_id int, first_name text, last_name text,
            PRIMARY KEY (song, user_id)
            )"""

try:
    session.execute(query3)
except Exception as e:
    print(e)
```

#### Insert data

The objective of query 3 is to extract a list of users who listen to a given song. Since people tend to play the same song many times, the event data is likely to contain multiple rows with the same user and song name. For this reason I preprocess the data in pandas to remove duplicates before inserting it in the Apache Cassandra table.


```python
file = 'event_datafile_new.csv'

df = pd.read_csv(file, usecols=[1, 4, 9, 10])
df.drop_duplicates(inplace=True)

for ix, row in df.iterrows():
    query = "INSERT INTO song_users (song, user_id, first_name, last_name)"
    query = query + " VALUES (%s, %s, %s, %s)"
    session.execute(query, (row['song'], row['userId'], row['firstName'], row['lastName']))
```

#### Run SELECT query to verify table model


```python
query3 = """SELECT first_name, last_name FROM song_users WHERE song = 'All Hands Against His Own'"""

try:
    rows = session.execute(query3)
except Exception as e:
    print(e)

for row in rows:
    print( row.first_name, row.last_name )
```

    Jacqueline Lynch
    Tegan Levine
    Sara Johnson


### Drop the tables before closing out the sessions


```python
drop_songs = "DROP TABLE IF EXISTS songs"
drop_user_sessions = "DROP TABLE IF EXISTS user_sessions"
drop_song_users = "DROP TABLE IF EXISTS song_users"
try:
    session.execute(drop_songs)
    session.execute(drop_user_sessions)
    session.execute(drop_song_users)
except Exception as e:
    print(e)
```

### Close the session and cluster connection¶


```python
session.shutdown()
cluster.shutdown()
```

#### Part II code was substantially completed by the student on base provided by instructors
