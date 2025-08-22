from pyspark.ml import PipelineModel
from pyspark.sql.functions import col, log
from pyspark.sql import SparkSession
from pyspark.sql.functions import round

# ---------- 1. Spark session ----------
print("🚀 Starting Spark session...")
spark = SparkSession.builder \
    .appName("FillMissingYears") \
    .config("spark.driver.memory", "28g") \
    .config("spark.executor.memory", "28g") \
    .config("spark.driver.maxResultSize", "8g") \
    .getOrCreate()
print("✅ Spark session started.")

# ---------- 2. Load full data ----------
print("📥 Loading full dataset...")
full_df = spark.read.format("avro").load("file:///home/senmrx/Documents/RecomMuse/data/music_data.avro")
print("✅ Full dataset loaded.")

# ---------- 3. Split into missing and valid year rows ----------
df_missing = full_df.filter((col("year").isNull()) | (col("year") == 0))
df_valid = full_df.filter(col("year").isNotNull() & (col("year") != 0))
print(f"✅ Rows with missing year: {df_missing.count()}")
print(f"✅ Rows with valid year:   {df_valid.count()}")

# ---------- 4. Load model and predict for missing rows ----------
print("📦 Loading model...")
model = PipelineModel.load("file:///home/senmrx/Documents/RecomMuse/year_regressor_final")
print("✅ Model loaded.")

print("🔧 Flattening features for missing rows...")
stat_names = ["mean", "median", "min", "max", "std", "count"]
for dim in range(12):
    for stat_idx, stat_name in enumerate(stat_names):
        col_idx = dim * 6 + stat_idx
        df_missing = df_missing.withColumn(f"timbre_{dim}_{stat_name}", col("segments_timbre")[col_idx])
        df_missing = df_missing.withColumn(f"pitch_{dim}_{stat_name}",  col("segments_pitches")[col_idx])

for i in range(78):
    df_missing = df_missing.withColumn(f"timbre_cov_{i}", col("timbre_cov")[i])
    df_missing = df_missing.withColumn(f"pitch_cov_{i}",  col("pitch_cov")[i])

stat_cols = [f"{p}_{d}_{s}" for p in ["timbre","pitch"] for d in range(12) for s in stat_names]
cov_cols  = [f"{p}_cov_{i}" for p in ["timbre","pitch"] for i in range(78)]
scalar_cols = [
    "artist_familiarity","artist_hotttnesss","song_hotttnesss",
    "duration","key","tempo","mode","loudness",
    "time_signature","key_confidence","mode_confidence"
]
df_missing = df_missing.withColumn("log_duration", log(col("duration") + 1e-3)) \
                       .withColumn("tempo_norm",   col("tempo") / 200.0)
scalar_cols += ["log_duration", "tempo_norm"]

feature_cols = stat_cols + cov_cols + scalar_cols
df_missing = df_missing.fillna(0.0, subset=feature_cols)
print("✅ Features flattened and filled.")

print("🧠 Running model prediction...")
predictions = model.transform(df_missing) \
                   .select("song_id", col("prediction").alias("year_predicted"))
print("✅ Predictions completed.")

# ---------- 5.  de-duplicate and merge ----------
print("🔗 De-duplicating and merging predicted years …")
# keep one prediction per song_id
predictions_unique = predictions.dropDuplicates(["song_id"])

df_missing_fixed = (
    predictions
        .dropDuplicates(["song_id"])                         # unique preds
        .withColumn("year_predicted", round("year_predicted").cast("int"))
        .join(
            df_missing.select("song_id", *df_valid.columns),  # original schema
            on="song_id",
            how="inner"
        )
        .drop("year")
        .withColumnRenamed("year_predicted", "year")
)

# ---------- 6.  union ----------
final_df = df_valid.unionByName(df_missing_fixed)
print(f"✅ Final dataset contains {final_df.count()} rows")   # should be 1 000 000

# ---------- 7. Save ----------
print("💾 Writing final Avro …")
final_df.coalesce(1) \
        .write.mode("overwrite") \
        .format("avro") \
        .save("file:///home/senmrx/Documents/RecomMuse/data/music_final_data.avro")
print("✅ Final dataset saved as music_final_data.avro")
