# Labraries
from kafka import KafkaProducer
import json, time, random, uuid
from datetime import datetime, timedelta

def run_orders_producer():
    # producer
    producer = KafkaProducer(
        bootstrap_servers='kafka:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    # Orders Data (Transactional Facts)
    def generate_orders(n=1000):
        customers = [f"CUST{str(i).zfill(4)}" for i in range(1, 201)]
        products = [
            {"id": "P001", "name": "Laptop", "price": 1200},
            {"id": "P002", "name": "Phone", "price": 800},
            {"id": "P003", "name": "Headphones", "price": 150},
            {"id": "P004", "name": "Monitor", "price": 300},
            {"id": "P005", "name": "Keyboard", "price": 60},
        ]
        start_date = datetime(2024, 1, 1)
        data = []
        for _ in range(n):
            cust = random.choice(customers)
            product = random.choice(products)
            qty = random.randint(1, 5)
            order_date = start_date + timedelta(days=random.randint(0, 180))
            data = {
                "order_id": str(uuid.uuid4()),
                "customer_id": cust,
                "product_id": product["id"],
                "product_name": product["name"],
                "quantity": qty,
                "price": product["price"],
                "total_value": qty * product["price"],
                "order_date": order_date.strftime("%Y-%m-%d")
            }
        return data
    # Send data to kafka
    while True:
        # Send data to "order_data" topic
        message_1 = generate_orders()
        producer.send("order_data", value=message_1)
        print("Sent:", message_1)
        time.sleep(1)