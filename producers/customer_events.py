# Labraries
from kafka import KafkaProducer
import json, time, random, uuid
from datetime import datetime, timedelta

# Producer
def run_customer_events_producer():
    producer = KafkaProducer(
        bootstrap_servers='kafka:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    # Customer Events (Behavioral Data)
    def generate_customer_events(n=1000):
        customers = [f"CUST{str(i).zfill(4)}" for i in range(1, 201)]
        actions = ["view_product", "add_to_cart", "remove_from_cart", "checkout", "wishlist"]
        products = ["P001", "P002", "P003", "P004", "P005"]
        data = []
        current_time = int(time.time())
        for _ in range(n):
            cust = random.choice(customers)
            action = random.choice(actions)
            prod = random.choice(products)
            timestamp = current_time - random.randint(0, 3600)
            data = {
                "event_id": str(uuid.uuid4()),
                "customer_id": cust,
                "product_id": prod,
                "action": action,
                "timestamp": timestamp
            }
        return data
    while True:
        # Send data to "customer_event" topic
        message_2 = generate_customer_events()
        producer.send("customer_event", value=message_2)
        print("Sent:", message_2)
        time.sleep(1)