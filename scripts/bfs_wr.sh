MAIN_DIR=/home/senmrx/Documents/RecomMuse

spark-submit \
  --jars $MAIN_DIR/lib/sqlite-jdbc-3.50.3.0.jar \
  --conf spark.driver.extraClassPath=$MAIN_DIR/lib/sqlite-jdbc-3.50.3.0.jar \
  $MAIN_DIR/src/artist_similarity/find_similar_artists.py $1 $2 $3
