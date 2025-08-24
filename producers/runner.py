import threading
import time
from ecommerce_orders import run_orders_producer
from customer_events import run_customer_events_producer

if __name__ == "__main__":
    t1 = threading.Thread(target=run_orders_producer, daemon=True)
    t2 = threading.Thread(target=run_customer_events_producer, daemon=True)

    t1.start()
    t2.start()

    # Keep main thread alive
    while True:
        time.sleep(1)
