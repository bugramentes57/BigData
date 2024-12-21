from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, FloatType

# Spark oturumu oluştur
spark = SparkSession.builder \
    .appName("Real-Time Anomaly Detection") \
    .config("spark.jars", "C:/Spark/spark-3.3.4-bin-hadoop3/jars/spark-sql-kafka-0-10_2.12-3.3.0.jar") \
    .getOrCreate()

# Kafka'dan veri okuyun
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "anomaly-data") \
    .load()

# Veri şeması
schema = StructType([
    StructField("field1", FloatType(), True),
    StructField("field2", FloatType(), True),
])

# Kafka verisini işleme
parsed_stream = kafka_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Konsola yazdırma
query = parsed_stream.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
