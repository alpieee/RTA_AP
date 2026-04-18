from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

for message in consumer:
    if message.value['amount']>1000:
        print(f"ALERT! Duża transakcja: {message.value['tx_id']} | "
              f"{message.value['amount']} PLN | {message.value['store']} | {message.value['category']}")
