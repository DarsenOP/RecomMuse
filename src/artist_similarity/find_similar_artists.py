"""
RDD-only BFS for ONE target artist (CLI version).
No driver-side loops over the graph.
"""

import argparse
import random
from pyspark.sql import SparkSession

def find_similar_artists_rdd_single(spark,
                                    db_path: str,
                                    start_artist_id: str,
                                    max_degrees: int,
                                    num_recommendations: int):
    """
    Returns [(artist_id, degree)] for a single starting artist.
    Uses only RDD transformations & actions.
    """
    # -------------------------------------------------------------
    # 1. Load edges as RDD[(src, dst)] – undirected
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
    # 2. Build initial frontier: (vertex, (root, degree))
    # -------------------------------------------------------------
    frontier = (
        edges
        .flatMap(lambda x: [x[0], x[1]])
        .distinct()
        .filter(lambda v: v == start_artist_id)
        .map(lambda v: (v, (start_artist_id, 0)))
    )

    # Accumulator: (root, vertex, degree)
    paths = frontier.map(lambda x: (start_artist_id, x[0], 0))

    # -------------------------------------------------------------
    # 3. Parallel BFS iterations
    # -------------------------------------------------------------
    for degree in range(1, max_degrees + 1):
        # frontier : (vertex, (root, prev_degree))
        # edges    : (vertex, neighbor)
        next_frontier = (
            frontier
            .join(edges)
            .map(lambda x: (x[1][1], (x[1][0][0], degree)))  # (neighbor, (root, degree))
            .distinct()
        )

        # Remove already-visited (root, vertex) pairs
        visited = paths.map(lambda x: (x[0], x[1]))
        next_frontier = (
            next_frontier
            .map(lambda x: ((x[1][0], x[0]), x[1][1]))   # ((root, vertex), degree)
            .subtract(visited.map(lambda x: (x, 1)))     # ((root, vertex), _)
            .map(lambda x: (x[0][0], x[0][1], x[1]))     # (root, vertex, degree)
        )

        paths = paths.union(next_frontier)
        frontier = next_frontier.map(lambda x: (x[1], (x[0], x[2])))

    # -------------------------------------------------------------
    # 4. Group, sample, and collect final recommendations
    # -------------------------------------------------------------
    recommendations = (
        paths
        .filter(lambda x: x[2] > 0)                             # drop start_artist_id
        .map(lambda x: (x[1], x[2]))                            # (vertex, degree)
        .groupByKey()                                           # (vertex, [degree, …])
        .mapValues(lambda ds: min(ds))                          # shortest degree
        .map(lambda x: (x[1], x[0]))                            # (degree, vertex)
        .groupByKey()                                           # (degree, [vertex, …])
        .flatMap(lambda d_vs: [(v, d_vs[0]) for v in d_vs[1]])  # flatten
    )

    random.seed(42)
    by_degree = {}
    for v, d in recommendations.collect():
        by_degree.setdefault(d, []).append(v)

    out = []
    for d in sorted(by_degree):
        needed = num_recommendations - len(out)
        if needed <= 0:
            break
        artists = by_degree[d]
        if len(artists) <= needed:
            out.extend([(v, d) for v in artists])
        else:
            out.extend([(v, d) for v in random.sample(artists, needed)])

    return out[:num_recommendations]

def parse_arguments():
    parser = argparse.ArgumentParser(description="Find similar artists (RDD-only BFS)")
    parser.add_argument('target_artist', type=str)
    parser.add_argument('max_depth', type=int)
    parser.add_argument('num_rec', type=int)
    parser.add_argument('--db_path',
                        default='/home/senmrx/Documents/RecomMuse/data/artist_similarity.db')
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_arguments()
    spark = SparkSession.builder.appName("ArtistSimilarityRDD-Single").getOrCreate()
    try:
        recs = find_similar_artists_rdd_single(
            spark,
            db_path=args.db_path,
            start_artist_id=args.target_artist,
            max_degrees=args.max_depth,
            num_recommendations=args.num_rec
        )

        print("\nRecommended Artists:")
        for artist_id, degree in recs:
            print(f"\t- {artist_id} (degree {degree})")
        print()
    finally:
        spark.stop()
