"""
Cloud function code to copy files from stage to curated buckets
"""

from google.cloud import storage


def mv_blob(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    """
    Function for moving files between directories or buckets. it will use GCP's copy 
    function then delete the blob from the old location.
    """
    file = event
    bucket_name = file['bucket']
    blob_name = file['name']
    # ex. 'data/some_location/file_name'
    new_bucket_name = file['bucket'].replace("staging", "curated")
    new_blob_name = file['name']
    # ex. 'data/destination/file_name'

    storage_client = storage.Client()
    source_bucket = storage_client.get_bucket(bucket_name)
    source_blob = source_bucket.blob(blob_name)
    destination_bucket = storage_client.get_bucket(new_bucket_name)

    # copy to new destination
    new_blob = source_bucket.copy_blob(
        source_blob, destination_bucket, new_blob_name)
    # delete in old destination
    source_blob.delete()

    print(f'File moved from {source_blob} to {new_blob_name}')


