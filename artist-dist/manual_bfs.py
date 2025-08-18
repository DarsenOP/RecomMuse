from pyspark.sql import SparkSession
from collections import deque
import random
import argparse

def find_similar_artists_manual(spark, db_path, start_artist_id, max_degrees, num_recommendations=10):
    """
    Finds similar artists using manual BFS implementation
    
    Args:
        spark: Active SparkSession
        db_path: Path to SQLite database
        start_artist_id: Artist ID to start from
        max_degrees: Maximum degrees of separation
        num_recommendations: Number of artists to return
        
    Returns:
        List of similar artist IDs with their degrees
    """
    # Load similarity data from SQLite
    similarity_df = spark.read.format("jdbc") \
        .option("url", f"jdbc:sqlite:{db_path}") \
        .option("dbtable", "similarity") \
        .load()
    
    # Convert to adjacency list format
    edges_rdd = similarity_df.select("target", "similar").rdd \
        .flatMap(lambda x: [(x[0], x[1]), (x[1], x[0])]) \
        .distinct() \
        .groupByKey() \
        .mapValues(list)
    
    # Collect adjacency list to driver (works for moderate-sized graphs)
    adj_list = dict(edges_rdd.collect())
    
    # BFS implementation
    visited = {start_artist_id: 0}
    queue = deque([start_artist_id])
    
    while queue:
        current = queue.popleft()
        current_degree = visited[current]
        
        if current_degree >= max_degrees:
            continue
            
        for neighbor in adj_list.get(current, []):
            if neighbor not in visited:
                visited[neighbor] = current_degree + 1
                queue.append(neighbor)
    
    # Remove the starting artist
    del visited[start_artist_id]
    
    # Group artists by degree
    artists_by_degree = {}
    for artist, degree in visited.items():
        if degree not in artists_by_degree:
            artists_by_degree[degree] = []
        artists_by_degree[degree].append(artist)
    
    # Select recommendations prioritizing closer degrees
    recommendations = []
    for degree in sorted(artists_by_degree.keys()):
        artists = artists_by_degree[degree]
        needed = num_recommendations - len(recommendations)
        
        if len(artists) <= needed:
            recommendations.extend((artist, degree) for artist in artists)
        else:
            # Random sample if too many at this degree
            recommendations.extend(
                (artist, degree) for artist in random.sample(artists, needed)
            )
        
        if len(recommendations) >= num_recommendations:
            break
    
    return recommendations[:num_recommendations]

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Find similar artists using BFS')
    parser.add_argument('target_artist', type=str, help='Artist ID to start from')
    parser.add_argument('max_depth', type=int, help='Maximum degrees of separation')
    parser.add_argument('num_rec', type=int, help='Number of recommendations to return')
    parser.add_argument('--db_path', type=str, default='/home/senmrx/Documents/RecomMuse/data/artist_similarity.db',
                      help='Path to SQLite database')
    return parser.parse_args()

# Usage Example
if __name__ == "__main__":
    args = parse_arguments()

    spark = SparkSession.builder \
        .appName("ArtistSimilarityManual") \
        .getOrCreate()
    
    try:
        results = find_similar_artists_manual(
            spark=spark,
            db_path=args.db_path,
            start_artist_id=args.target_artist,
            max_degrees=args.max_depth,
            num_recommendations=args.num_rec
        )
        
        print("\nRecommended Artists:")
        for artist_id, degree in results:
            print(f"- {artist_id} (degree {degree})")
        print()
    finally:
        spark.stop()
