"""
Pretty-print every recommendation stored in Redis.
Run:  python dump_recommendations.py > recommendations.txt
"""
import redis
import json
import argparse

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--redis_host", default="localhost")
    parser.add_argument("--redis_port", type=int, default=6379)
    args = parser.parse_args()

    r = redis.Redis(host=args.redis_host, port=args.redis_port, decode_responses=True)

    # Scan every key that matches our pattern
    for key in r.scan_iter(match="artist:*:recs"):
        target = key.split(":")[1]            # extract TARGET id
        recs = json.loads(r.get(key))         # [(rec_id, degree), ...]

        print(f"{target}:")
        cur_rec = 0
        for rec_id, degree in recs:
            print(f"\t- {rec_id} (degree {degree})")
            cur_rec += 1

        print()   # blank line between targets

if __name__ == "__main__":
    main()
