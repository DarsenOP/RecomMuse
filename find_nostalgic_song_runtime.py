#!/usr/bin/env python3
"""
spark-submit find_nostalgic_song.py  <song_id>  [<k>]

Example:
spark-submit find_nostalgic_song.py  SOAAAWQ12A8C1420B5  5
"""

import sys, math, random, argparse
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
# ------------------------------------------------------------------
# 1. Import your existing BFS function (copy-pasted from earlier)
# ------------------------------------------------------------------
from find_similar_artists import find_similar_artists_rdd_single   # adjust module name if needed

# ------------------------------------------------------------------
# 2. Nostalgia score (year only)
# ------------------------------------------------------------------
CURRENT_YEAR = date.today().year

def nostalgia_score(year: int, listener_birth_year: int = 1990) -> float:
    """
    Very small, interpretable formula:
        peak_nostalgia_age = 16
        nostalgia_weight   = gaussian centered at listener_birth_year + 16
    """
    if year is None:
        return -1e9
    peak_year = listener_birth_year + 16
    sigma = 5.0
    return math.exp(-0.5 * ((year - peak_year) / sigma) ** 2)

# ------------------------------------------------------------------
# 3. Glue code
# ------------------------------------------------------------------
def find_nostalgic_song(spark,
                        avro_path: str,
                        db_path: str,
                        start_song_id: str,
                        listener_birth_year: int,
                        top_k: int = 1):
    # -------------------------------------------------------------
    # 3a. Map song_id -> artist_id
    # -------------------------------------------------------------
    songs = (
        spark.read.format("avro").load(avro_path)
        .select("song_id", "artist_id", "title", "artist_name", "year")
    )
    row = songs.filter(col("song_id") == start_song_id).first()
    if not row:
        raise ValueError(f"Song {start_song_id} not found")
    artist_id = row.artist_id
    print(f"Anchor song: {row.title} by {row.artist_name} ({row.year})")

    # -------------------------------------------------------------
    # 3b. Similar artists via existing BFS
    # -------------------------------------------------------------
    similar = find_similar_artists_rdd_single(
        spark,
        db_path=db_path,
        start_artist_id=artist_id,
        max_degrees=2,
        num_recommendations=10
    )
    similar_artist_set = {aid for aid,_ in similar}
    print(f"Found {len(similar_artist_set)} similar artists")

    # -------------------------------------------------------------
    # 3c. All songs from similar artists
    # -------------------------------------------------------------
    candidate_songs = (
        songs
        .filter(col("artist_id").isin(similar_artist_set))
        .filter(col("song_id") != start_song_id)
        .collect()
    )
    print(f"Candidate pool: {len(candidate_songs)} songs")

    # -------------------------------------------------------------
    # 3d. Score & pick
    # -------------------------------------------------------------
    scored = [
        (nostalgia_score(s.year, listener_birth_year), s)
        for s in candidate_songs
    ]
    scored.sort(key=lambda x: x[0], reverse=True)
    return scored[:top_k]

# ------------------------------------------------------------------
# CLI
# ------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("song_id")
    parser.add_argument("k", nargs="?", type=int, default=1)
    parser.add_argument("--avro", default="/user/senmrx/input/music_data.avro")
    parser.add_argument("--db",   default="/home/senmrx/Documents/RecomMuse/data/artist_similarity.db")
    parser.add_argument("--birth-year", type=int, default=1990,
                        help="Listener birth year (tweak for demo)")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("NostalgicSong").getOrCreate()
    try:
        hits = find_nostalgic_song(
            spark,
            avro_path=args.avro,
            db_path=args.db,
            start_song_id=args.song_id,
            listener_birth_year=args.birth_year,
            top_k=args.k
        )
        print("\nMost nostalgic songs:")
        for score, s in hits:
            print(f"{score:.3f}  {s.title} - {s.artist_name} ({s.year})")
    finally:
        spark.stop()
