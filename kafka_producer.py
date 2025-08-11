# producer.py
import json
import random
import time
from datetime import datetime
import requests
from kafka import KafkaProducer

TOPIC = 'orders'
BOOTSTRAP = 'localhost:9092'
CATALOG_URL = 'https://dummyjson.com/products?limit=100'

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def load_catalog():
    try:
        r = requests.get(CATALOG_URL, timeout=10)
        r.raise_for_status()
        return r.json().get('products', [])
    except Exception as e:
        print('Catalog fetch failed:', e)
        return []

def generate_order(product, order_id):
    qty = random.randint(1, 5)
    return {
        'order_id': f'O{order_id}',
        'product_id': str(product.get('id')),
        'product_name': product.get('title'),
        'price': float(product.get('price') or 0.0),
        'quantity': qty,
        'total_amount': float(product.get('price') or 0.0) * qty,
        'user_id': f'U{random.randint(100,999)}',
        'order_timestamp': datetime.utcnow().isoformat(),
        'location': random.choice(['Delhi','Mumbai','Bengaluru','Kolkata','Chennai'])
    }

if __name__ == '__main__':
    catalog = load_catalog()
    if not catalog:
        print('No catalog data — exit')
        exit(1)
    order_id = 1000
    print('Producer started — sending orders to', TOPIC)
    try:
        while True:
            prod = random.choice(catalog)
            order = generate_order(prod, order_id)
            producer.send(TOPIC, value=order)
            print('Sent', order['order_id'], order['product_name'], '₹', order['total_amount'])
            order_id += 1
            time.sleep(random.uniform(0.5, 2.0))
    except KeyboardInterrupt:
        print('Stopping producer')
        producer.flush()
        producer.close()
