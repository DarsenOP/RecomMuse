# RecomMuse
RecomMuse - Your soundtrack to the past, presents the future

RecomMuse is an intelligent song recommendation system that bridges nostalgia and discovery. By analyzing user preferences and listening history, it curates personalized playlists blending nostalgic favorites with fresh, forward-thinking tracks—turning memories into music and anticipation into melody.

## Key Features

- **Nostalgia-Driven Recommendations** – Rediscover forgotten gems tailored to your past tastes.
- **Future-Focused Suggestions** – Explore new music aligned with your evolving preferences.
- **Dynamic Personalization** – ML-powered insights refine recommendations over time.
- **User-Centric Design** – Intuitive interface for seamless exploration.

# Start of the project (Milestone 1 : Environment Setup)

- Hadoop/Spark/Drill/Java/Maven were configured successfully
    - The guide on how to check if everything works can be seen below

Here is how to run all the components and make everything work together:

1. To start Hadoop with HDFS and YARN we run 
```bash 
start-all.sh # start-dfs.sh && start-yarn.sh
```
2. After that we can run Drill by just typing 
```bash 
docker compose up
```
then connect to the terminal SQL line with 
```bash 
./run-drill.sh 
```
3. Here is how we can run Spark with Avro
```bash 
spark-submit \ 
  --master yarn \
  --packages org.apache.spark:spark-avro_2.13:4.0.0 \
  --conf spark.yarn.appMasterEnv.JAVA_HOME=/usr/lib/jvm/java-17-openjdk \
  --conf spark.executorEnv.JAVA_HOME=/usr/lib/jvm/java-17-openjdk \
  <FILENAME>.py
```
4. And lastly Maven. To generate a hierarchy in maven use 
```bash 
mvn archetype:generate \
  -DgroupId=com.example \
  -DartifactId=maven-test \
  -DarchetypeArtifactId=maven-archetype-quickstart \
  -DinteractiveMode=false
```
Then do the needed modification to the `pom.xml` file and finally run the following commands to compile and run it
```bash 
mvn clean package 
java -jar target/<JARNAME>.jar 
```

# Data Preparation (Milestone 2)

- Because the data was given as lots of small binary files (HDF5 files), which is not ideal for HDFS (because the blocksize is 128MB), the compaction is needed
- The Avro was chosen as the most fitting one
- Avro schema was written to capture the maximum information
- AvroWriter/Reader java files were written to 
    - Read from HDF5 file and write it to Avro
    - Read from Avro itself and extract information 
- The Snappy Codec was used for memory efficiency (~99.6% in my case)

Here is how to run the compaction using the scripts written:
```bash
cd data-prep
mvn clean package 
java -jar <JARNAME>.jar <INPUT_DIR> <OUTPUT_DIR>
```

Then to read from the Avro file, the `AvroReader.java` can be modified accordingly and run
```bash 
java -jar <JARNAME>.jar <INPUT_FILE>.avro
```

# Simple Queries (Milestone 3) 

After the tools and data are ready, I tried to query simple tasks, to just get into Drill a little, and most importantly understand if year prediction system is needed. Here are the results:
- Oldest and Newest song
```bash 
+------------------------------------+------+
|               title                | year |
+------------------------------------+------+
| Warm And Sunny Day                 | 1922 |
| Warm And Sunny Day                 | 1922 |
| Something In My Heart (Full Vocal) | 1922 |
| Don't Pan Me                       | 1922 |
| Mandela You're Free                | 1922 |
| Looking My Love                    | 1922 |
+------------------------------------+------+

+----------+------+
|  title   | year |
+----------+------+
| Popinjay | 2011 |
+----------+------+
```
- Hottest song that is the shortest and shows highest energy with lowest tempo
```bash 
+------------------+-----------------+-----------+--------+---------+
|       title      | song_hotttnesss | duration  | energy |  tempo  |
+------------------+-----------------+-----------+--------+---------+
| Jingle Bell Rock | 1.0             | 120.63302 | 0.0    | 128.711 |
+------------------+-----------------+-----------+--------+---------+
```
- Album with the most tracks in it 
```bash 
+---------------+--------+
|    release    | ntrack |
+---------------+--------+
| Greatest Hits | 2014   |
+---------------+--------+
```
- Name of the artist with the longest song
```bash 
+--------------------------------+------------+
|          artist_name           |  duration  |
+--------------------------------+------------+
| Mystic Revelation of Rastafari | 3034.90567 |
+--------------------------------+------------+
```
- Lastly the percentage of songs without year attribute 
```bash 
+--------+
| EXPR$0 |
+--------+
| 48.44  |
+--------+
```

> NOTE: The SQL queries can be found in `./simple-queries/queries.sql`

The most important one is the last one, which suggests that **Year Prediction System Is Essential**

# Artist Distance Graph (Milestone 4)

There are few features that were added: Add recommendation data to REDIS, do an instant lookup from REDIS db, and a simple BFS in a Spark. 

For the first one we can run 
```bash 
spark-submit \
    --jars <JAR_NAME> \
    --conf spark.driver.extraClassPath=<JAR_NAME> \
    build_redis_cache.py --max_degrees <MAX_DEGREES> --sample_size <SAMPLE_SIZE>
```

Now in order to check and print the REDIS db we can run the following one:
```bash 
python3 dump_recommendations.py
```

and we get the following output 
```bash 
ARZPOKA11F4C83B829:
	- AR1HDB11187FB56058 (degree 1)
	- AR24K5Z1187B9B4BFC (degree 1)
	- AR2CXDY1187B9B4EA8 (degree 1)
	- AR3TZ691187FB3DBB1 (degree 1)
	- AR3ZYST1187B9B0707 (degree 1)
  ...

ARYUXMD11F50C4AB52:
	- AR19DTT1187FB3FA4A (degree 1)
	- AR4Y7WE1187B98FA3F (degree 1)
	- AR992GR1187FB3C814 (degree 1)
	- ARBGCAL11EBCD75B65 (degree 1)
	- ARBRWVP1241B9CC9C0 (degree 1)
  ...

ARISMYC11F50C5046D:
	- AR00L9V1187FB4353A (degree 1)
	- AR0B6OD1187B9ABED2 (degree 1)
	- AR1LJAZ1187FB5AF93 (degree 1)
	- AR2DGLV1187FB59329 (degree 1)
	- AR2JZY41187B9B6239 (degree 1)
  ...

...
```

To check the recommendation for a certain artist we can run 
```bash 
python3 artist_recs.py <TARGET_ARTIST> <NUM_OF_RECOMMENDATIONS>
```

to get the following output for (ARZPOKA11F4C83B829, 8)
```bash 
Recommended Artists:
	- AR1HDB11187FB56058 (degree 1)
	- AR24K5Z1187B9B4BFC (degree 1)
	- AR2CXDY1187B9B4EA8 (degree 1)
	- AR3TZ691187FB3DBB1 (degree 1)
	- AR3ZYST1187B9B0707 (degree 1)
	- AR4K7X81187FB4BEAD (degree 1)
	- AR6XONI1187B98DD54 (degree 1)
	- AR7IYWW1187FB483F6 (degree 1)
```

And finally to run a BFS without REDIS db from a certain artist using 
```bash 
spark-submit \
    --jars <JAR_NAME> \
    --conf spark.driver.extraClassPath=<JAR_NAME> \
    find_similar_artists.py <TARGET_ARTIST> <MAX_DEGREES> <NUM_OF_RECOMMENDATIONS>
```

and got the same type of output.
