"""
artist_recs.py
Instant lookup from Redis (no Spark, no SQL).
"""
import argparse
import redis
import json

def main(artist_id, k, redis_host="localhost", redis_port=6379):
    r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
    data = r.get(f"artist:{artist_id}:recs")
    if not data:
        print("Artist not found in cache")
        return
    recs = json.loads(data)[:k]
    
    return recs

if __name__ == "__main__":    
    parser = argparse.ArgumentParser()
    parser.add_argument("artist_id")
    parser.add_argument("k", type=int)
    parser.add_argument("--redis_host", default="localhost")
    parser.add_argument("--redis_port", type=int, default=6379)
    args = parser.parse_args()

    recs = main()
    print("\nRecommended Artists:")
    for aid, deg in recs:
        print(f"\t- {aid} (degree {deg})")
