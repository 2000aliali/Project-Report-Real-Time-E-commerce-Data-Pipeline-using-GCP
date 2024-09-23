import json
import time
import random
from datetime import datetime
from google.cloud import pubsub_v1, bigquery
from google.oauth2 import service_account

# Set the path for the service account key
credentials = service_account.Credentials.from_service_account_file(
    r"C:\Users\Lenovo\Desktop\GCP_Projet\dataengineering-project-2024-c303494e3939.json"
)

# Initialize Pub/Sub client with credentials
project_id = "dataengineering-project-2024"
topic_id = "realtime-dashboard-data"
publisher = pubsub_v1.PublisherClient(credentials=credentials)
topic_path = publisher.topic_path(project_id, topic_id)

# Initialize BigQuery client
bq_client = bigquery.Client(credentials=credentials, project=project_id)

# Define BigQuery table names
user_table_id = f"{project_id}.ecommerce.user_table"
product_table_id = f"{project_id}.ecommerce.product_table"
location_table_id = f"{project_id}.ecommerce.location_table"

# Predefined data for dimension tables
users = [ 
    {'user_id': 'user_1', 'name': 'Alice', 'email': 'alice@example.com', 'signup_date': '2023-01-15'},
    {'user_id': 'user_2', 'name': 'Bob', 'email': 'bob@example.com', 'signup_date': '2023-02-10'},
    {'user_id': 'user_4', 'name': 'Charlie', 'email': 'charlie@example.com', 'signup_date': '2023-03-05'},
    {'user_id': 'user_5', 'name': 'Ali', 'email': 'ali@example.com', 'signup_date': '2023-03-05'},
    {'user_id': 'user_6', 'name': 'Yassine', 'email': 'yassine@example.com', 'signup_date': '2023-03-05'},
    {'user_id': 'user_7', 'name': 'Omare', 'email': 'omare@example.com', 'signup_date': '2023-03-05'},
    {'user_id': 'user_8', 'name': 'Sana', 'email': 'sana@example.com', 'signup_date': '2023-03-05'},
    {'user_id': 'user_9', 'name': 'Walid', 'email': 'walid@example.com', 'signup_date': '2023-03-05'},
    {'user_id': 'user_10', 'name': 'Ayobe', 'email': 'ayobe@example.com', 'signup_date': '2022-03-05'},
    {'user_id': 'user_11', 'name': 'Hamez', 'email': 'hamez@example.com', 'signup_date': '2023-03-05'},
    {'user_id': 'user_12', 'name': 'David', 'email': 'david@example.com', 'signup_date': '2023-04-01'},
    {'user_id': 'user_13', 'name': 'Eva', 'email': 'eva@example.com', 'signup_date': '2023-04-15'},
    {'user_id': 'user_14', 'name': 'Frank', 'email': 'frank@example.com', 'signup_date': '2023-05-10'},
    {'user_id': 'user_15', 'name': 'Grace', 'email': 'grace@example.com', 'signup_date': '2023-05-20'},
    {'user_id': 'user_16', 'name': 'Henry', 'email': 'henry@example.com', 'signup_date': '2024-06-05'},
    {'user_id': 'user_17', 'name': 'Isabella', 'email': 'isabella@example.com', 'signup_date': '2024-06-15'}
]

products = [
    {'product_id': 'prod_1', 'product_name': 'Laptop', 'category': 'Electronics', 'price': 1200.00},
    {'product_id': 'prod_2', 'product_name': 'Phone', 'category': 'Electronics', 'price': 800.00},
    {'product_id': 'prod_3', 'product_name': 'Headphones', 'category': 'Accessories', 'price': 150.00},
    {'product_id': 'prod_4', 'product_name': 'Smartwatch', 'category': 'Accessories', 'price': 300.00},
    {'product_id': 'prod_5', 'product_name': 'Tablet', 'category': 'Electronics', 'price': 600.00},
    {'product_id': 'prod_6', 'product_name': 'Camera', 'category': 'Electronics', 'price': 900.00},
    {'product_id': 'prod_7', 'product_name': 'Wireless Mouse', 'category': 'Accessories', 'price': 50.00},
    {'product_id': 'prod_8', 'product_name': 'Keyboard', 'category': 'Accessories', 'price': 80.00}
]

locations = [
    {'location_id': 'loc_1', 'city': 'New York', 'state': 'NY', 'country': 'USA'},
    {'location_id': 'loc_2', 'city': 'Toronto', 'state': 'ON', 'country': 'Canada'},
    {'location_id': 'loc_3', 'city': 'Paris', 'state': 'Île-de-France', 'country': 'France'},
    {'location_id': 'loc_4', 'city': 'Tokyo', 'state': 'Tokyo', 'country': 'Japan'},
    {'location_id': 'loc_5', 'city': 'London', 'state': 'England', 'country': 'UK'},
    {'location_id': 'loc_6', 'city': 'Berlin', 'state': 'Berlin', 'country': 'Germany'},
    {'location_id': 'loc_7', 'city': 'Sydney', 'state': 'NSW', 'country': 'Australia'},
    {'location_id': 'loc_8', 'city': 'Dubai', 'state': 'Dubai', 'country': 'UAE'}
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


# Insert data into BigQuery tables
def insert_data_into_bigquery(table_id, rows):
    errors = bq_client.insert_rows_json(table_id, rows)
    if errors == []:
        print(f"New rows have been added to {table_id}")
    else:
        print(f"Encountered errors while inserting rows: {errors}")


# Publish data to Pub/Sub
def publish_to_pubsub(data):
    data_str = json.dumps(data)
    data_bytes = data_str.encode('utf-8')
    future = publisher.publish(topic_path, data=data_bytes)
    print(f"Published message ID: {future.result()}")


if __name__ == "__main__":
    try:
        # Insert predefined dimension table data into BigQuery
        insert_data_into_bigquery(user_table_id, users)
        insert_data_into_bigquery(product_table_id, products)
        insert_data_into_bigquery(location_table_id, locations)

        # Uncomment this if you want to simulate and publish transactions
        for i in range(20):
            transaction = generate_transaction_data()
            print(f"Publishing transaction: {transaction}")
            publish_to_pubsub(transaction)
            time.sleep(random.randint(1, 5))
    except KeyboardInterrupt:
        print("Stopped publishing data.")
