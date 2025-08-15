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
- The Snappy Codec was used for memory efficiency (~60% in my case)

Here is how to run the compaction using the scripts written:
```bash
cd data-prep
mvn clean package 
java -jar <JARNAME>.jar <INPUT_DIR> <OUTPUT_DIR>
```

For me the whole compaction took nearly 3 hours.

Then to read from the Avro file, the `AvroReader.java` can be modified accordingly and run
```bash 
java -jar <JARNAME>.jar <INPUT_FILE>.avro
```
