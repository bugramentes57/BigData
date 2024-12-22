from kafka import KafkaProducer
import pandas as pd
import json
import time

# Kafka producer oluştur
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Veri setini oku
data = pd.read_csv('datafolder/anomalytagliveriseti.csv')

# Kafka'ya gönderilecek sütunları seç
data = data[['record', '0_pre-RR', '0_post-RR', '0_qrs_interval', 'anomaly']]

# Kafka'ya veri gönder
for _, row in data.iterrows():
    message = {
        "record": row['record'],
        "pre_rr": row['0_pre-RR'],
        "post_rr": row['0_post-RR'],
        "qrs_interval": row['0_qrs_interval'],
        "anomaly": row['anomaly']
    }
    producer.send("anomaly-data", value=message)
    print(f"Sent: {message}")
    time.sleep(0.5)

producer.close()
