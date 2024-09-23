import argparse
import json
from datetime import datetime
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.bigquery import WriteToBigQuery

# Function to parse Pub/Sub messages
def parse_pubsub_message(message):
    data = json.loads(message)
    # Convert timestamp to a valid BigQuery format
    data['timestamp'] = datetime.strptime(data['timestamp'], '%Y-%m-%dT%H:%M:%S.%f').isoformat()
    return data

def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', required=True, help='GCP Project ID')
    parser.add_argument('--subscription', required=True, help='Pub/Sub Subscription')
    parser.add_argument('--temp_location', required=True, help='GCS Temp Location')
    parser.add_argument('--staging_location', required=True, help='GCS Staging Location')
    parser.add_argument('--runner', required=True, help='Dataflow runner')
    args = parser.parse_args()

    options = PipelineOptions(
        runner=args.runner,
        project=args.project,
        temp_location=args.temp_location,
        staging_location=args.staging_location
    )

    # Define the pipeline
    with beam.Pipeline(options=options) as p:
        (
            p
            | 'ReadFromPubSub' >> ReadFromPubSub(subscription=args.subscription)
            | 'ParseMessages' >> beam.Map(parse_pubsub_message)
            | 'WriteToBigQuery' >> WriteToBigQuery(
                table='data-ali-project-2024:ecommerce.fact_transactions',
                schema='transaction_id:STRING, timestamp:TIMESTAMP, user_id:STRING, product_id:STRING, location_id:STRING, amount:FLOAT',
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )

if __name__ == '__main__':
    run()
