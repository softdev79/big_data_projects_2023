"""
Cloud function code to trigger data flow template to insert data from csv file to Bigtable.
"""

from googleapiclient.discovery import build
import google.auth
import os
import datetime


def run_dataflow(event, context):
    file = event
    bucket_name = file['bucket']
    blob_name = file['name']
    filepath = "gs://"+bucket_name+"/"+blob_name
    if (blob_name.split('/')[0]=='data'):
        credentials, _ = google.auth.default()
        service = build('dataflow', 'v1b3', credentials=credentials)
        gcp_project = os.environ["GCP_PROJECT"]

        template_path = "gs://dezyre-curated/batchtemplate"
        template_body = {
            "job_name":"gcsbt_"+str(datetime.datetime.now()),
            "parameters": {
                "input_path": filepath,
                "bigtable_project": gcp_project,
                "bigtable_instance": "dezyre",
                "bigtable_table": "covid",
            },
            "environment": {
                "machineType": "n1-standard-4"
            }
        }

        request = service.projects().templates().launch(projectId=gcp_project, gcsPath=template_path, body=template_body)
        response = request.execute()
        print(response)

