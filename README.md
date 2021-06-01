# sparkifydb Data Lake

Sparkify is a music streaming app, in general,
they want to analyze the data they've been collecting on songs and user activity.

# AWS
## CLI
Install the AWS Command Line Interface by running:
```
sudo apt update && sudo apt install -y awscli
```

### EMR from AWS CLI
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

## [EMR from AWS Console](https://classroom.udacity.com/nanodegrees/nd027/parts/19ef4e55-151f-4510-8b5c-cb590ac52df2/modules/f268ecf3-99fa-4f44-8587-dfa0945b8a7f/lessons/1f8f1b41-f5aa-4276-93f7-ec4916a74ed5/concepts/eac5c2be-645d-4d58-b7ac-a2dc02268e7e)

Data is originally stored in an S3 bucket, but in order to analyse it and serve it, a Spark cluster is used.

## Schema
Schema is broadly divided in two main parts: song data and log data.

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

### Running
Run the Spark job as follows:
```
spark-submit etl.py
```

## Project requirements
Requirements can be found in `requirements.txt`