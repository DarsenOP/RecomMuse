from pyspark.sql import SparkSession
from pyspark.sql.functions import col, log
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml import Pipeline
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.evaluation import RegressionEvaluator

# ========== 1 ========== #

spark = SparkSession.builder \
    .appName("YearPrediction") \
    .config("spark.driver.memory", "28g") \
    .config("spark.executor.memory", "28g") \
    .config("spark.driver.maxResultSize", "8g") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.memory.fraction", "0.6") \
    .config("spark.memory.storageFraction", "0.3") \
    .getOrCreate()

# ------------- verify at runtime -------------
print("Driver heap limit:",
      spark.sparkContext.getConf().get("spark.driver.memory"))
print("Executor heap limit:",
      spark.sparkContext.getConf().get("spark.executor.memory"))

print("✅ Spark initialized successfully!")

# ========== 2 ========== #

df = spark.read.format("avro").load("file:///home/senmrx/Documents/RecomMuse/data/music_data.avro") \
    .select(
        "segments_timbre",      # 72 features
        "segments_pitches",     # 72 features
        "timbre_cov",           # 78 features
        "pitch_cov",            # 78 features

        "artist_familiarity",
        "artist_hotttnesss",
        "song_hotttnesss",
        "duration",
        "key",
        "tempo",
        "mode",
        "loudness",
        "time_signature",
        "key_confidence",
        "mode_confidence",

        "year"                  # Target variable
    )

print("✅ Data loaded successfully!")

# ========== 3 ========== #

df = df.filter((col("year").isNotNull()) & (col("year") != 0))
df = df.filter((col("year") >= 1970))

print("✅ Data cleaning completed successfully!")

# ========== 4 ========== #

# --- 1. flatten the 144 stats (12 dims × 6 stats) -----------------
stat_names = ["mean", "median", "min", "max", "std", "count"]

for dim in range(12):
    for stat_idx, stat_name in enumerate(stat_names):
        col_idx = dim * 6 + stat_idx
        df = df.withColumn(f"timbre_{dim}_{stat_name}", col("segments_timbre")[col_idx])
        df = df.withColumn(f"pitch_{dim}_{stat_name}",  col("segments_pitches")[col_idx])

# --- 2. flatten the 78-element covariance vectors -----------------
for i in range(78):
    df = df.withColumn(f"timbre_cov_{i}", col("timbre_cov")[i])
    df = df.withColumn(f"pitch_cov_{i}",  col("pitch_cov")[i])

print("✅ Data flattening completed successfully!")

# ========== 5 ========== #


stat_cols = [
    f"{prefix}_{dim}_{stat}"
    for prefix in ["timbre", "pitch"]
    for dim in range(12)
    for stat in ["mean", "median", "min", "max", "std", "count"]
]

cov_cols = [
    f"{prefix}_cov_{i}"
    for prefix in ["timbre", "pitch"]
    for i in range(78)
]

scalar_cols = [
    "artist_familiarity", 
    "artist_hotttnesss", 
    "song_hotttnesss",
    "duration", 
    "key", 
    "tempo", 
    "mode", 
    "loudness",
    "time_signature", 
    "key_confidence", 
    "mode_confidence"
]

df = df.withColumn("log_duration", log(col("duration") + 1e-3)) \
       .withColumn("tempo_norm", col("tempo") / 200.0)
scalar_cols.extend(["log_duration", "tempo_norm"])

feature_cols = stat_cols + cov_cols + scalar_cols
label_col = ["year"]

df = df.select(feature_cols + label_col)

print("✅ Feature columns assembled successfully!")

# ========== 6 ========== #

df = df.fillna(0.0, subset=feature_cols)

print("✅ Rows with null/NaN dropped successfully!")

# ========== 7 ========== #

assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

print("✅ Assembling finished successfully!")

# ========== 8 ========== #

gbt = GBTRegressor(
    featuresCol="features",
    labelCol="year",
    maxIter=150,
    maxDepth=8,
    stepSize=0.05,
    seed=42
)

print("✅ Regressor created successfully!")

# ========== 9 ========== #

pipeline = Pipeline(stages=[assembler, gbt])

print("✅ Pipeline created successfully!")

# ========== 10 ========== #

train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
print("✅ Train / test split finished successfully!")

# ========== 11 ========== #

model = pipeline.fit(train_df)
model.save("file:///home/senmrx/Documents/RecomMuse/year_regressor_final")
print("✅ Model fitted successfully and saved!")
