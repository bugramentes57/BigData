from kafka import KafkaProducer
import pandas as pd
import json
import time

# Veri dosyasını yükle
data_path = "ecg.csv"
data = pd.read_csv(data_path)

# Kafka Producer oluştur
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name = "anomaly-data"

# Verileri Kafka'ya gönder
for index, row in data.iterrows():
    message = row.to_dict()
    producer.send(topic_name, message)
    print(f"Sent: {message} + osuruk")
    time.sleep(0.1)  # Veri akışını simüle etmek için bekleme

producer.close()
