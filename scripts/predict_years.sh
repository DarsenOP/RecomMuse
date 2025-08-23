YEAR_PREDICTION_DIR=/home/senmrx/Documents/RecomMuse/src/year_prediction

spark-submit \
  --packages org.apache.spark:spark-avro_2.13:4.0.0 \
  --conf spark.yarn.appMasterEnv.JAVA_HOME=/usr/lib/jvm/java-17-openjdk \
  --conf spark.executorEnv.JAVA_HOME=/usr/lib/jvm/java-17-openjdk \
  --conf "spark.driver.extraJavaOptions=-Dlog4j.configurationFile=/opt/spark-4.0.0/spark-4.0.0-bin-hadoop3/conf/log4j2.properties" \
  --conf "spark.executor.extraJavaOptions=-Dlog4j.configurationFile=/opt/spark-4.0.0/spark-4.0.0-bin-hadoop3/conf/log4j2.properties" \
  --executor-memory 8g \
  --driver-memory 8g \
  $YEAR_PREDICTION_DIR/predict_years.py 2>/dev/null
