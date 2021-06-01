# sparkifydb Data Lake

Sparkify is a music streaming app, in general, they want to analyze the data they've been collecting on songs and user activity.

# AWS CLI
Install the AWS Command Line Interface by running:
```
sudo apt update && sudo apt install -y awscli
```

## EMR from AWS CLI
```
aws emr create-cluster 
    --name spark-udacity
    --use-default-roles 
    --release-label emr-5.28.0 
    --instance-count 3 
    --applications Name=Spark 
    --ec2-attributes KeyName=spark-cluster-1 
    --instance-type m5.xlarge 
    --auto-terminate
``` 


Data is originally stored in an S3 bucket, but in order to analyse it and serve it, a Redshift cluster is used.
## Schema
Schema is broadly divided in two main parts, the first one is a replica of the data stored in S3 as SQL tables, the second one contains the OLAP tables.
### Staging
- Events: `staging_events`
  - 
### OLAP
- Fact Table: `songplays`
    - records in log data associated with song plays i.e. records with page NextSong
        - songplay_id
        - start_time
        - user_id
        - level
        - song_id
        - artist_id
        - session_id
        - location
        - user_agent

- Dimension Tables:
    - `users` - users in the app
        - user_id
        - first_name
        - last_name
        - gender
        - level
    - `songs` - songs in music database
        - song_id
        - title
        - artist_id
        - year
        - duration
    - `artists` - artists in music database
        - artist_id
        - name
        - location
        - latitude
        - longitude
    - `time` - timestamps of records in songplays broken down into specific units
        - start_time
        - hour
        - day
        - week
        - month
        - year
        - weekday
### Running
The process that creates the schema can be run as follows:
```
python create_tables.py
```

## ETL
The logical process by which the data is uploaded to the database is composed by two steps:
- `create_tables.py`
    - `CREATE` schema
    - `DROP` drop schema
- `etl.py`
    - populates the entire database by using the data available in `/data/`
    
### Running
First create required tables with:
```
python create_tables.py
```

Then run the ETL process as follows:
```
python etl.py
```

## Project requirements
Requirements can be found in `requirements.txt`