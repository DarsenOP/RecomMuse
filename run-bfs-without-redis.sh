spark-submit \
  --jars /home/senmrx/Documents/RecomMuse/lib/sqlite-jdbc-3.50.3.0.jar \
  --conf spark.driver.extraClassPath=/home/senmrx/Documents/RecomMuse/lib/sqlite-jdbc-3.50.3.0.jar \
  ./artist-dist/find_similar_artists.py $1 $2 $3
