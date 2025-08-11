# processor.py
import json
import time
from kafka import KafkaConsumer
from sqlalchemy import create_engine, Table, Column, Integer, String, Float, DateTime, MetaData
from sqlalchemy.orm import registry, sessionmaker
from datetime import datetime
import os

TOPIC = 'orders'
BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', 'localhost:9092')
DB_URL = os.getenv('DB_URL', 'sqlite:///orders.db')

engine = create_engine(DB_URL, echo=False, future=True)
metadata = MetaData()

orders_table = Table('orders', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('order_id', String, unique=True, nullable=False),
    Column('product_id', String),
    Column('product_name', String),
    Column('price', Float),
    Column('quantity', Integer),
    Column('total_amount', Float),
    Column('user_id', String),
    Column('order_timestamp', DateTime),
    Column('location', String),
    Column('received', String, default='no')
)

metadata.create_all(engine)

mapper_registry = registry()

class Order:
    def __init__(self, **kw):
        for k, v in kw.items():
            setattr(self, k, v)

mapper_registry.map_imperatively(Order, orders_table)
Session = sessionmaker(bind=engine, future=True)

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    consumer_timeout_ms=1000
)

print('Processor running — listening to', TOPIC)
try:
    session = Session()
    while True:
        for msg in consumer:
            data = msg.value
            try:
                if not data.get('order_id') or data.get('price') is None:
                    print('Invalid order skipped', data)
                    continue
                ts = data.get('order_timestamp')
                try:
                    order_dt = datetime.fromisoformat(ts)
                except Exception:
                    order_dt = datetime.utcnow()
                order_obj = Order(
                    order_id = data.get('order_id'),
                    product_id = str(data.get('product_id')),
                    product_name = data.get('product_name'),
                    price = float(data.get('price') or 0),
                    quantity = int(data.get('quantity') or 0),
                    total_amount = float(data.get('total_amount') or 0),
                    user_id = data.get('user_id'),
                    order_timestamp = order_dt,
                    location = data.get('location'),
                    received = 'no'
                )
                existing = session.execute(
                    orders_table.select().where(orders_table.c.order_id == order_obj.order_id)
                ).first()
                if existing:
                    print('Duplicate skipped', order_obj.order_id)
                else:
                    session.add(order_obj)
                    session.commit()
                    print('Saved', order_obj.order_id)
            except Exception as e:
                print('Processing error:', e)
        time.sleep(0.5)
except KeyboardInterrupt:
    print('Processor stopped by user')
    consumer.close()
    session.close()
