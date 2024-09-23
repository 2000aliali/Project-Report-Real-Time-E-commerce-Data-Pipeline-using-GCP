import json
import time
import random
from datetime import datetime
from google.cloud import pubsub_v1

# Initialize Pub/Sub client
project_id = "data-ali-project-2024"  # Replace with your GCP project ID
topic_id = "realtime-dashboard-data"  # Replace with your Pub/Sub topic ID
subscription_id = "realtime-dashboard-data-sub"  # Your subscription ID

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

# Predefined data for dimension tables
users = [
    {'user_id': 'user_1', 'name': 'Alice', 'email': 'alice@example.com', 'signup_date': '2023-01-15'},
    {'user_id': 'user_2', 'name': 'Bob', 'email': 'bob@example.com', 'signup_date': '2023-02-10'},
    {'user_id': 'user_3', 'name': 'Charlie', 'email': 'charlie@example.com', 'signup_date': '2023-03-05'}
]

products = [
    {'product_id': 'prod_1', 'product_name': 'Laptop', 'category': 'Electronics', 'price': 1200.00},
    {'product_id': 'prod_2', 'product_name': 'Phone', 'category': 'Electronics', 'price': 800.00},
    {'product_id': 'prod_3', 'product_name': 'Headphones', 'category': 'Accessories', 'price': 150.00}
]

locations = [
    {'location_id': 'loc_1', 'city': 'New York', 'state': 'NY', 'country': 'USA'},
    {'location_id': 'loc_2', 'city': 'Toronto', 'state': 'ON', 'country': 'Canada'},
    {'location_id': 'loc_3', 'city': 'Paris', 'state': 'Île-de-France', 'country': 'France'}
]

# Simulate real-time e-commerce transaction data (fact table)
def generate_transaction_data():
    user = random.choice(users)
    product = random.choice(products)
    location = random.choice(locations)

    transaction_data = {
        'transaction_id': f'trans_{random.randint(1000, 9999)}',
        'timestamp': datetime.utcnow().isoformat(),
        'user_id': user['user_id'],
        'product_id': product['product_id'],
        'location_id': location['location_id'],
        'amount': product['price'] * round(random.uniform(0.5, 1.5), 2)  # Random discount
    }

    return transaction_data

# Publish data to Pub/Sub
def publish_to_pubsub(data):
    data_str = json.dumps(data)
    data_bytes = data_str.encode('utf-8')
    
    # Publish data
    future = publisher.publish(topic_path, data=data_bytes)
    print(f"Published message ID: {future.result()}")

# Function to process incoming messages
def callback(message):
    print(f"Received message: {message.data}")
    message.ack()  # Acknowledge the message

if __name__ == "__main__":
    # Start a background subscriber
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}...\n")

    try:
        for i in range(300):
            transaction = generate_transaction_data()
            print(f"Publishing transaction: {transaction}")
            publish_to_pubsub(transaction)
            
            # Simulate real-time events (every 1-5 seconds)
            time.sleep(random.randint(1, 5))
    except KeyboardInterrupt:
        print("Stopped publishing data.")
        streaming_pull_future.cancel()  # Cancel the future
