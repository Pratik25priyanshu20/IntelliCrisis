from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
import json

# Setup Spark
spark = SparkSession.builder.appName("DeltaConsumer").getOrCreate()

# Delta table path
DELTA_PATH = "/mnt/data/delta/news_articles"

def store_to_delta(message):
    try:
        message["timestamp"] = str(datetime.utcnow())
        df = spark.read.json(spark.sparkContext.parallelize([json.dumps(message)]))
        df.write.format("delta").mode("append").save(DELTA_PATH)
        print(f"✅ Stored in Delta Lake: {message.get('title')[:80]}")
    except Exception as e:
        print(f"❌ Delta write error: {e}")