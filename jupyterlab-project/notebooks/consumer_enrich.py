from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

for message in consumer:
    if message.value['amount']>3000:
        risk_level="HIGH"
    elif message.value['amount']>1000:
        risk_level="MEDIUM"
    else:
        risk_level="LOW"

    message.value['risk_level']=risk_level
        
    print(message.value)
