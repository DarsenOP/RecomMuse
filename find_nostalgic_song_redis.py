#!/usr/bin/env python3
"""
spark-submit find_nostalgic_song.py

Example:
spark-submit find_nostalgic_song.py
"""

import sys, math, random
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from artist_recs import main

# ------------------------------------------------------------------
# Nostalgia score function
# ------------------------------------------------------------------
CURRENT_YEAR = date.today().year

def nostalgia_score(year: int, listener_birth_year: int = 1990) -> float:
    if year is None:
        return -1e9
    peak_year = listener_birth_year + 16
    sigma = 5.0
    return math.exp(-0.5 * ((year - peak_year) / sigma) ** 2)

# ------------------------------------------------------------------
# Fallback function: get top nostalgic songs from entire database
# ------------------------------------------------------------------
def get_fallback_nostalgic_songs(songs_df, listener_birth_year: int, top_k: int):
    """Fallback when no similar artists found - get most nostalgic songs overall"""
    print("🎯 No similar artists found. Getting most nostalgic songs from entire database...")
    
    # Collect all songs and calculate nostalgia scores
    all_songs = songs_df.collect()
    scored = [
        (nostalgia_score(s.year, listener_birth_year), s)
        for s in all_songs
    ]
    scored.sort(key=lambda x: x[0], reverse=True)
    
    return scored[:top_k]

# ------------------------------------------------------------------
# Main interactive function
# ------------------------------------------------------------------
def interactive_nostalgic_recommender(spark, avro_path: str, db_path: str):
    # Load data ONCE at startup
    print("Loading music data...")
    songs = (
        spark.read.format("avro").load(avro_path)
        .select("song_id", "artist_id", "title", "artist_name", "year")
    )
    print(f"✅ Loaded {songs.count()} songs")
    
    # Interactive loop
    while True:
        try:
            print("\n" + "="*50)
            print("NOSTALGIC SONG RECOMMENDER")
            print("="*50)
            
            # Get user input
            start_song_id = input("Enter song ID (or 'quit' to exit): ").strip()
            if start_song_id.lower() in ['quit', 'exit', 'q']:
                break
                
            top_k = int(input("Number of recommendations: ").strip())
            listener_birth_year = int(input("Your birth year: ").strip())
            
            # Find the anchor song
            row = songs.filter(col("song_id") == start_song_id).first()
            if not row:
                print(f"❌ Song {start_song_id} not found. Try another ID.")
                continue
                
            artist_id = row.artist_id
            print(f"🎵 Anchor song: {row.title} by {row.artist_name} ({row.year})")
            
            # Get similar artists
            print("🔍 Finding similar artists...")
            similar = main(artist_id, 10)
            
            if not similar:
                # Fallback: No similar artists found
                recommendations = get_fallback_nostalgic_songs(songs, listener_birth_year, top_k)
                print(f"🎯 Found {len(recommendations)} nostalgic songs from entire database")
                
            else:
                similar_artist_set = {aid for aid, _ in similar}
                print(f"✅ Found {len(similar_artist_set)} similar artists")
                
                # Get candidate songs from similar artists
                candidate_songs = (
                    songs
                    .filter(col("artist_id").isin(similar_artist_set))
                    .filter(col("song_id") != start_song_id)
                    .collect()
                )
                
                if not candidate_songs:
                    # Fallback: Similar artists but no other songs
                    print("🎯 Similar artists found but no other songs available. Getting nostalgic songs from database...")
                    recommendations = get_fallback_nostalgic_songs(songs, listener_birth_year, top_k)
                else:
                    print(f"🎯 Candidate pool: {len(candidate_songs)} songs from similar artists")
                    
                    # Score and recommend from similar artists
                    scored = [
                        (nostalgia_score(s.year, listener_birth_year), s)
                        for s in candidate_songs
                    ]
                    scored.sort(key=lambda x: x[0], reverse=True)
                    recommendations = scored[:top_k]
            
            # Display results
            print(f"\n🎧 Top {len(recommendations)} Nostalgic Recommendations:")
            print("-" * 60)
            for i, (score, song) in enumerate(recommendations, 1):
                print(f"{i}. {score:.3f} - {song.title} by {song.artist_name} ({song.year})")
                
        except ValueError as e:
            print(f"❌ Invalid input: {e}")
        except Exception as e:
            print(f"❌ Error: {e}")

# ------------------------------------------------------------------
# Main execution
# ------------------------------------------------------------------
if __name__ == "__main__":
    # Initialize Spark once
    spark = SparkSession.builder \
        .appName("NostalgicSongRecommender") \
        .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.5.0") \
        .getOrCreate()
    
    try:
        # Start interactive session
        interactive_nostalgic_recommender(
            spark,
            avro_path="file:///home/senmrx/Documents/RecomMuse/data/music_data.avro",
            db_path="/home/senmrx/Documents/RecomMuse/data/artist_similarity.db"
        )
    finally:
        spark.stop()
        print("👋 Session ended. Thank you for using our program!")
