# RecomMuse
RecomMuse - Your soundtrack to the past, presents the future

RecomMuse is an intelligent song recommendation system that bridges nostalgia and discovery. By analyzing user preferences and listening history, it curates personalized playlists blending nostalgic favorites with fresh, forward-thinking tracks—turning memories into music and anticipation into melody.

## Key Features

- **Nostalgia-Driven Recommendations** – Rediscover forgotten gems tailored to your past tastes.
- **Future-Focused Suggestions** – Explore new music aligned with your evolving preferences.
- **Dynamic Personalization** – ML-powered insights refine recommendations over time.
- **User-Centric Design** – Intuitive interface for seamless exploration.

# Start of the project (Milestone 1 : Environment Setup)

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

