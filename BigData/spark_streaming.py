from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json, struct
from pyspark.sql.types import StructType, StructField, FloatType

# Spark oturumu oluştur
spark = SparkSession.builder \
    .appName("ECG Anomaly Detection") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Kafka ayarları
kafka_brokers = "localhost:9092"
input_topic = "anomaly-data"
anomaly_output_topic = "anomaly-alerts"
normal_output_topic = "normal-data"

# Kafka veri şeması
schema = StructType([
    StructField("record", FloatType(), True),
    StructField("pre_rr", FloatType(), True),
    StructField("post_rr", FloatType(), True),
    StructField("qrs_interval", FloatType(), True),
    StructField("anomaly", FloatType(), True)
])

# Kafka'dan okunan veriyi stream'e dönüştür
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_brokers) \
    .option("subscribe", input_topic) \
    .option("startingOffsets", "earliest") \
    .load()

# JSON veriyi ayrıştır
parsed_stream = raw_stream.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

# Treshold belirle
threshold = 10

# Anomalileri filtrele
anomalies = parsed_stream.filter((col("qrs_interval") > threshold) | (col("anomaly") == 1.0)) \
    .withColumn("value", to_json(struct("record", "pre_rr", "post_rr", "qrs_interval", "anomaly")))

# Normal veriyi filtrele
normal_data = parsed_stream.filter((col("qrs_interval") <= threshold) & (col("anomaly") == 0.0)) \
    .withColumn("value", to_json(struct("record", "pre_rr", "post_rr", "qrs_interval", "anomaly")))

# Anomalileri Kafka'ya yaz
anomalies.select("value").writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_brokers) \
    .option("topic", anomaly_output_topic) \
    .option("checkpointLocation", "checkpoint-anomalies") \
    .start()

# Normal veriyi Kafka'ya yaz
normal_data.select("value").writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_brokers) \
    .option("topic", normal_output_topic) \
    .option("checkpointLocation", "checkpoint-normal") \
    .start()

# Konsola yazdır (isteğe bağlı)
query = parsed_stream.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
