from pyspark.sql import SparkSession, Row

spark = SparkSession.builder.appName("WordCount").getOrCreate()

# read
lines = spark.sparkContext.textFile("hdfs://localhost:9000/user/senmrx/input/drill_test.txt")

# word-count
counts_rdd = (lines
              .flatMap(lambda l: l.split())
              .map(lambda w: (w, 1))
              .reduceByKey(lambda a, b: a + b))

# convert to DataFrame so we can write Avro
counts_df = counts_rdd.map(lambda wc: Row(word=wc[0], count=wc[1])).toDF()

# save as Avro
counts_df.write.mode("overwrite").format("avro").save("hdfs://0.0.0.0:9000/data/spark_avro")

spark.stop()
