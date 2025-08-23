# RecomMuse 🎧  
**End-to-end nostalgia-driven music recommender**  
*Built after finishing freshman year – data pipelines, Spark, Redis, and a touch of sentiment.* 

## 🚀 Demo

![RecomMuse Demo](docs/assets/demo.gif)

---

## 📌 Overview  
RecomMuse takes a **song** or **artist** and returns a **nostalgic recommendation** by:  
1. Predicting the release year for tracks that lack it (Spark GBT).  
2. Performing a breadth-first search on an artist-similarity graph (Spark RDD -> Redis).  
3. Selecting the song from a similar artist whose release year is closest to “listener was 16”.

---

## 🧰 Prerequisites  
| Component | Purpose |
|-----------|---------|
| **Java** | Spark & Drill runtime |
| **Maven** | Build Java helpers |
| **Python** | CLI, Redis client |
| **Spark** | Distributed BFS & ML |
| **Hadoop** | HDFS backend for Apache Drill |
| **Redis** | Low-latency neighbour cache |
| **Apache Drill** | SQL exploration on HDFS/Avro |

> 🔧 **Need help installing Hadoop/Spark/Drill?**  
> See [Issue #1](https://github.com/DarsenOP/RecomMuse/issues/1) – step-by-step setup is explained there.

---

## 📊 Data Preparation  
1. **HDF5 -> Avro**  
   ```bash
   cd data-preparation
   mvn clean package
   # Convert an entire folder
   java -jar target/<JAR_NAME_WITH_DEPENDENCIES> /path/to/h5_folder /path/to/output_folder
   # Or inspect a single file
   java -jar target/<JAR_NAME_WITH_DEPENDENCIES> /path/to/single.avro
   ```

2. Start Drill & HDFS
   ```bash
   start-all.sh
   docker compose up
   ./run-drill.sh
   # Copy drill_condig_web.conf into Drill Web UI -> Storage -> dfs -> Update
   ```

## 🚀 Running RecomMuse

1. 🔧 One-time environment
    ```bash
    chmod +x setup_env.sh
    ./setup_env.sh
    source .venv/bin/activate  
    ```

2. 🧠 Year pipeline (first time only)
    ```bash
    ./run_train_model.sh    # train GBT regressor
    ./run_evaluate_model.sh # optional RMSE/R² check
    ./run_predict_years.sh  # fill missing years
    ```

3. 🎶 Artist similarity (pick one)

| Command                                        | Description                                      |
| ---------------------------------------------- | ------------------------------------------------ |
| `./run_build_redis_cache.sh`                   | Pre-compute neighbours for random sample → Redis |
| `./run_find_similar_artists.sh ARTIST_ID 3 10` | On-demand BFS (Spark)                            |
| `./run_artist_recs.sh ARTIST_ID 5`             | Instant Redis lookup                             |
| `./run_dump_recommendations.sh`                | List every cached artist                         |

4. 🎁 Final nostalgic recommender

Run `RecomMuse.sh`

## License
MIT – hack away and make it yours.

> NOTE: To see the roadmap of the progress and how all went you can check [project roadmap](./docs/ROADMAP.md).
