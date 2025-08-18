spark-submit \
  --jars /home/senmrx/Documents/RecomMuse/lib/sqlite-jdbc-3.50.3.0.jar \
  --conf spark.driver.extraClassPath=/home/senmrx/Documents/RecomMuse/lib/sqlite-jdbc-3.50.3.0.jar \
  ./artist-dist/build_redis_cache.py --max_degrees $1 --sample_size $2
