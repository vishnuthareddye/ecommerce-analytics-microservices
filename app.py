import json
import time
import queue
import threading
from datetime import datetime

# Global Thread-Safe Queue to simulate a Message Broker (like 
Apache Kafka
 / 
RabbitMQ
  )
message_broker = queue.Queue()

class OrderIngestionService(threading.Thread):
    """Microservice 1: Ingests order webhooks from storefronts and publishes to the broker."""
    def __init__(self):
        super().__init__()
        self.daemon = True
        
    def create_mock_transaction(self, order_id, item, cost):
        payload = {
            "order_id": order_id,
            "timestamp": datetime.utcnow().isoformat(),
            "item_name": item,
            "transaction_value": cost
        }
        print(f"[Ingestion Service] Successfully captured Order #{order_id} - ${cost}")
        message_broker.put(json.dumps(payload))

class RevenueAnalyticsService(threading.Thread):
    """Microservice 2: Consumes transactional payloads from the broker and updates financials."""
    def __init__(self):
        super().__init__()
        self.daemon = True
        self.total_revenue = 0.0
        self.processed_count = 0
        
    def run(self):
        while True:
            try:
                # Blocks until an item is available in the message broker pipeline
                event_data = message_broker.get()
                transaction = json.loads(event_data)
                
                # Process the data packet asynchronously
                self.total_revenue += transaction["transaction_value"]
                self.processed_count += 1
                
                print(f"[Analytics Service] PROCESSED: Metrics Updated. Total Revenue: ${self.total_revenue:.2f} | Tx Count: {self.processed_count}")
                message_broker.task_done()
            except Exception as e:
                print(f"[Analytics Service] Processing Error: {e}")
                break

if __name__ == "__main__":
    print("--- Launching Microservices Cluster ---")
    
    # Spin up worker services asynchronously
    ingestion_node = OrderIngestionService()
    analytics_node = RevenueAnalyticsService()
    
    analytics_node.start()
    
    # Simulate real-time continuous transactions pushing into the architecture
    mock_items = [("Cloud Server Cluster", 1200.00), ("AI Developer License", 450.00), ("Premium Hardware Gateway", 89.99)]
    
    for i, (item, cost) in enumerate(mock_items, start=1001):
        ingestion_node.create_mock_transaction(i, item, cost)
        time.sleep(1.5)  # Interval separation mimicking live distributed traffic
        
    # Wait for the processing queue to drain cleanly
    message_broker.join()
    print("--- All Asynchronous Events Successfully Formatted ---")
