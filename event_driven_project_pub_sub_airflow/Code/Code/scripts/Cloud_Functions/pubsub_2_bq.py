"""
Cloud function code to copy messages from pubsub to big query.
"""

import base64
from google.cloud import bigquery
import csv

def pubsub_2_bq(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    bq_client = bigquery.Client()
    dataset_id = 'dezyre'
    table_id = 'covid_stream'
    table_ref = bq_client.dataset(dataset_id).table(table_id)
    table = bq_client.get_table(table_ref)
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    print(pubsub_message)
    reader = csv.reader([pubsub_message],quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL, skipinitialspace=True)
    #how many rows are comming in this line?
    for row in reader:
            tuple_row = tuple(row)
            row_to_insert = [ tuple_row ]
            errors = bq_client.insert_rows(table, row_to_insert)
            if errors != []:
                print(row_to_insert)
                print(errors)
