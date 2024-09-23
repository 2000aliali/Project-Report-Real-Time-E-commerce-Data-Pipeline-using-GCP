import argparse
import json
from datetime import datetime
from apache_beam.transforms.window import FixedWindows 
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.bigquery import WriteToBigQuery


# Function to parse Pub/Sub messages
def parse_pubsub_message(message):
    data = json.loads(message)
    
    # Convert timestamp to a valid BigQuery format (ISO 8601)
    if 'timestamp' in data:
        data['timestamp'] = datetime.strptime(data['timestamp'], '%Y-%m-%dT%H:%M:%S.%f').isoformat()

    return data

# Functions to filter messages by table type
def filter_fact_transactions(message):
    return message['table'] == 'fact_transactions'

def filter_dim_users(message):
    return message['table'] == 'dim_users'

def filter_dim_products(message):
    return message['table'] == 'dim_products'

def filter_dim_locations(message):
    return message['table'] == 'dim_locations'


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', required=True, help='GCP Project ID')
    parser.add_argument('--subscription', required=True, help='Pub/Sub Subscription')
    parser.add_argument('--temp_location', required=True, help='GCS Temp Location')
    parser.add_argument('--staging_location', required=True, help='GCS Staging Location')
    parser.add_argument('--runner', required=True, help='Dataflow runner')
    parser.add_argument('--region', required=True, help='GCP Region')
    args = parser.parse_args()

    options = PipelineOptions(
        runner=args.runner,
        project=args.project,
        temp_location=args.temp_location,
        staging_location=args.staging_location
    )


    with beam.Pipeline(options=options) as p:

        options = PipelineOptions()

        # Read messages from Pub/Sub
        messages = (p
                    | 'ReadFromPubSub' >> ReadFromPubSub(subscription=args.subscription)
                    | 'ParseMessages' >> beam.Map(parse_pubsub_message)
                    | 'WindowIntoFixed' >> beam.WindowInto(FixedWindows(300)))  # Apply a 5-minute window

        # Write to fact_transactions table
        (messages
         | 'FilterFactTransactions' >> beam.Filter(filter_fact_transactions)
         | 'WriteToFactTransactions' >> WriteToBigQuery(
            table='data-ali-project-2024:ecommerce.fact_transactions',
            schema='transaction_id:STRING, timestamp:TIMESTAMP, user_id:STRING, product_id:STRING, location_id:STRING, amount:FLOAT',
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        ))

        # Write to dim_users table
        (messages
         | 'FilterDimUsers' >> beam.Filter(filter_dim_users)
         | 'WriteToDimUsers' >> WriteToBigQuery(
            table='data-ali-project-2024:ecommerce.dim_users',
            schema='user_id:STRING, name:STRING, email:STRING, signup_date:DATE',
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        ))

        # Write to dim_products table
        (messages
         | 'FilterDimProducts' >> beam.Filter(filter_dim_products)
         | 'WriteToDimProducts' >> WriteToBigQuery(
            table='data-ali-project-2024:ecommerce.dim_products',
            schema='product_id:STRING, product_name:STRING, category:STRING, price:FLOAT64',
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        ))

        # Write to dim_locations table
        (messages
         | 'FilterDimLocations' >> beam.Filter(filter_dim_locations)
         | 'WriteToDimLocations' >> WriteToBigQuery(
            table='data-ali-project-2024:ecommerce.dim_locations',
            schema='location_id:STRING, city:STRING, state:STRING, country:STRING',
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        ))

        """ """
        """ # Define the pipeline
    with beam.Pipeline(options=options) as p:
        # Read messages from Pub/Sub
        messages = (p
                    | 'ReadFromPubSub' >> ReadFromPubSub(subscription=args.subscription)
                    | 'ParseMessages' >> beam.Map(parse_pubsub_message))

        # Write to fact_transactions table
        (messages
         | 'FilterFactTransactions' >> beam.Filter(filter_fact_transactions)
         | 'WriteToFactTransactions' >> WriteToBigQuery(
            table='data-ali-project-2024:ecommerce.fact_transactions',
            schema='transaction_id:STRING, timestamp:TIMESTAMP, user_id:STRING, product_id:STRING, location_id:STRING, amount:FLOAT',
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        ))

        # Write to dim_users table
        (messages
         | 'FilterDimUsers' >> beam.Filter(filter_dim_users)
         | 'WriteToDimUsers' >> WriteToBigQuery(
            table='data-ali-project-2024:ecommerce.dim_users',
            schema='user_id:STRING, name:STRING, email:STRING, signup_date:DATE',
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        ))

        # Write to dim_products table
        (messages
         | 'FilterDimProducts' >> beam.Filter(filter_dim_products)
         | 'WriteToDimProducts' >> WriteToBigQuery(
            table='data-ali-project-2024:ecommerce.dim_products',
            schema='product_id:STRING, product_name:STRING, category:STRING, price:FLOAT64',
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        ))

        # Write to dim_locations table
        (messages
         | 'FilterDimLocations' >> beam.Filter(filter_dim_locations)
         | 'WriteToDimLocations' >> WriteToBigQuery(
            table='data-ali-project-2024:ecommerce.dim_locations',
            schema='location_id:STRING, city:STRING, state:STRING, country:STRING',
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        ))
"""       
if __name__ == '__main__':
    run()
