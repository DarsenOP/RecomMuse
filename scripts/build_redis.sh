MAIN_DIR=/home/senmrx/Documents/RecomMuse

# spark-submit \
#   --jars $MAIN_DIR/lib/sqlite-jdbc-3.50.3.0.jar \
#   --conf spark.driver.extraClassPath=$MAIN_DIR/lib/sqlite-jdbc-3.50.3.0.jar \
#   $MAIN_DIR/src/artist_similarity/build_redis_cache.py --max_degrees 3 --sample_size 1

python3 $MAIN_DIR/src/artist_similarity/dump_recommendations.py >tmp.txt
echo "The artist that can be used $(head -n 1 tmp.txt)"
rm tmp.txt
