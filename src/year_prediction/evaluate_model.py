"""
evaluate_saved_model.py
Load the persisted GBT model, score the test split, print metrics.
No training, no grid search, no CrossValidator.
"""
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import col, log

# ---------- 0. Spark ----------
print("🚀 Starting Spark session...")
spark = SparkSession.builder \
    .appName("EvaluateSavedYearPredictor") \
    .getOrCreate()
print("✅ Spark session started.")

# ---------- 1. Load test data (same cleaning as training) ----------
print("📥 Loading test dataset...")
df = spark.read.format("avro").load("file:///home/senmrx/Documents/RecomMuse/data/music_data.avro") \
    .filter(col("year").isNotNull() & (col("year") != 0)) \
    .filter(col("year") >= 1970)
print("✅ Test dataset loaded.")

# ---------- 2. Re-create feature columns (identical to training) ----------
print("🔧 Flattening features for missing rows...")
# 1. flatten 144 stats
stat_names = ["mean", "median", "min", "max", "std", "count"]
for dim in range(12):
    for stat_idx, stat_name in enumerate(stat_names):
        col_idx = dim * 6 + stat_idx
        df = df.withColumn(f"timbre_{dim}_{stat_name}", col("segments_timbre")[col_idx])
        df = df.withColumn(f"pitch_{dim}_{stat_name}",  col("segments_pitches")[col_idx])

# 2. flatten 78 covariances
for i in range(78):
    df = df.withColumn(f"timbre_cov_{i}", col("timbre_cov")[i])
    df = df.withColumn(f"pitch_cov_{i}",  col("pitch_cov")[i])

# 3. scalar + engineered
df = df.withColumn("log_duration", log(col("duration") + 1e-3)) \
       .withColumn("tempo_norm",   col("tempo") / 200.0)

print("✅ Features flattened and filled.")

# ---------- 3. Load saved model ----------
print("📦 Loading model...")
model = PipelineModel.load("file:///home/senmrx/Documents/RecomMuse/year_regressor_final")
print("✅ Model loaded.")

# ---------- 4. Predict ----------
print("🧠 Running model prediction...")
_, test_df = df.randomSplit([0.8, 0.2], seed=42)
pred_df = model.transform(test_df)
print("✅ Predictions completed.")

# ---------- 5. Evaluate ----------
print("🔗 Starting the evaluation of the model.")
evaluator = RegressionEvaluator(labelCol="year", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(pred_df)
print(f"RMSE (years): {rmse:.2f}")

for metric in ["r2", "mae"]:
    evaluator.setMetricName(metric)
    print(f"{metric.upper()}: {evaluator.evaluate(pred_df):.3f}")

print("✅ Evaluation completed.")

spark.stop()
