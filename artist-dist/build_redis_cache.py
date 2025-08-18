"""
RDD-only BFS, but only for a random subset of artists.
"""
import argparse
import redis
import json
from pyspark.sql import SparkSession

def parallel_bfs_sample(spark,
                        db_path: str,
                        max_degrees: int = 3,
                        sample_size: int = 5,
                        redis_host: str = "localhost",
                        redis_port: int = 6379):
    # -------------------------------------------------------------
    # 1. Load edges as RDD[(src, dst)]
    # -------------------------------------------------------------
    edges_df = (
        spark.read.format("jdbc")
        .option("url", f"jdbc:sqlite:{db_path}")
        .option("dbtable", "similarity")
        .load()
    )

    edges = (
        edges_df
        .select("target", "similar")
        .rdd
        .flatMap(lambda row: [(row.target, row.similar),
                              (row.similar, row.target)])
        .distinct()
    )

    # -------------------------------------------------------------
    # 2. Pick a random sample of vertices
    # -------------------------------------------------------------
    vertices = edges.flatMap(lambda x: [x[0], x[1]]).distinct()
    sample_artists = vertices.takeSample(False, sample_size)        # driver-side, tiny
    sample_bc = spark.sparkContext.broadcast(set(sample_artists))   # broadcast

    # -------------------------------------------------------------
    # 3. Build initial frontier only for the sample
    #    (vertex, (root, degree))
    # -------------------------------------------------------------
    frontier = (
        vertices
        .filter(lambda v: v in sample_bc.value)
        .map(lambda v: (v, (v, 0)))
    )

    # Accumulator for paths: (root, neighbor, degree)
    paths = frontier.map(lambda x: (x[0], x[0], 0))

    # -------------------------------------------------------------
    # 4. BFS iterations
    # -------------------------------------------------------------
    for degree in range(1, max_degrees + 1):
        next_frontier = (
            frontier
            .join(edges)
            .map(lambda x: (x[1][1], (x[1][0][0], degree)))
            .distinct()
        )

        visited = paths.map(lambda x: (x[0], x[1]))
        next_frontier = (
            next_frontier
            .map(lambda x: ((x[1][0], x[0]), x[1][1]))
            .subtract(visited.map(lambda x: (x, 1)))
            .map(lambda x: (x[0][0], x[0][1], x[1]))
        )

        paths = paths.union(next_frontier)
        frontier = next_frontier.map(lambda x: (x[1], (x[0], x[2])))

    # -------------------------------------------------------------
    # 5. Group and push only sampled roots to Redis
    # -------------------------------------------------------------
    grouped = (
        paths
        .filter(lambda x: x[2] > 0)
        .map(lambda x: (x[0], (x[1], x[2])))
        .groupByKey()
        .mapValues(lambda vs: sorted(vs, key=lambda t: (t[1], t[0])))
    )

    results = grouped.collect()          # small list
    r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
    r.flushdb()
    pipe = r.pipeline()
    for root, recs in results:
        pipe.set(f"artist:{root}:recs", json.dumps(recs))
    pipe.execute()
    print(f"Cached {len(results)} sampled artists to Redis")

# -------------------------------------------------------------
# Entry point
# -------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db_path", default="/home/senmrx/Documents/RecomMuse/data/artist_similarity.db")
    parser.add_argument("--max_degrees", type=int, default=3)
    parser.add_argument("--sample_size", type=int, default=5)
    parser.add_argument("--redis_host", default="localhost")
    parser.add_argument("--redis_port", type=int, default=6379)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("RedisCacheRDD-Sample").getOrCreate()
    try:
        parallel_bfs_sample(spark,
                            db_path=args.db_path,
                            max_degrees=args.max_degrees,
                            sample_size=args.sample_size,
                            redis_host=args.redis_host,
                            redis_port=args.redis_port)
    finally:
        spark.stop()
