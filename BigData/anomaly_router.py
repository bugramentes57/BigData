from kafka import KafkaConsumer, KafkaProducer
import json
import numpy as np

# Kafka Consumer ve Producer'ları tanımlayın
consumer = KafkaConsumer(
    'anomaly-data',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Anomali tespiti için eşik değeri
ANOMALY_THRESHOLD = 3.0

# Gelen verileri işleyip ilgili topic'e gönderin
for message in consumer:
    data = message.value

    # Örnek bir tespit: Verilerin ortalama değerine göre anomali kontrolü
    values = np.array(list(data.values()), dtype=float)
    mean_value = np.mean(values)

    if mean_value > ANOMALY_THRESHOLD:
        producer.send('anomalies', data)  # Anomaliler topic'ine gönder
        print(f"Anomaly detected and sent: {data}")
    else:
        producer.send('normal-data', data)  # Normal veriler topic'ine gönder
        print(f"Normal data sent: {data}")
