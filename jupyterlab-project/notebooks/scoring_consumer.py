from kafka import KafkaConsumer, KafkaProducer
import json

consumer = KafkaConsumer('transactions', bootstrap_servers='broker:9092',
    auto_offset_reset='earliest', group_id='scoring-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))

alert_producer = KafkaProducer(bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def score_transaction(tx):
    score = 0
    rules = []

    # Reguła R1: Bardzo wysoka kwota (>3000)
    if tx['amount']> 3000:
        score += 3
        rules.append('R1')

    # Reguła R2: Elektronika + wysoka kwota(>1500)
    if tx['category'] == 'elektronika' and tx['amount'] > 1500:
        score += 2
        rules.append('R2')
        
    # Reguła R3: Godzina nocna
    if tx.get('hour', 0) < 6: #godzina 0-5
        score += 2
        rules.append('R3')
  
    return score, rules

for message in consumer:
    tx = message.value
    score, rules =score_transaction(tx)
    if score>=3:
        tx['score']=score
        tx['rules']=rules
        alert_producer.send('alerts', value=tx)
        print(f"ALERT: {tx['tx_id']} kwota {tx['amount']}, rules={rules}")

alert_producer.flush()
alert_producer.close()
