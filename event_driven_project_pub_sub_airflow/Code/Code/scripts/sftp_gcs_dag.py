"""
Example Airflow DAG for Google Cloud Storage to SFTP transfer operators.
"""

import os

from airflow import models
from airflow.operators.dummy import DummyOperator
#from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.transfers.sftp_to_gcs import SFTPToGCSOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

#BUCKET_SRC = "dezyre-staging"
BUCKET_SRC = Variable.get("dest_bucket")
TMP_PATH = "/home"
DIR = "shrutijoshi"
SUBDIR = "data"

OBJECT_SRC_1 = "file-1.bin"
OBJECT_SRC_2 = "file-2.bin"
OBJECT_SRC_3 = "file-3.txt"

with models.DAG("sftp_to_gcs", start_date=days_ago(1), schedule_interval=None) as dag:
    start = DummyOperator(
        task_id='start'
    )

    end = DummyOperator(
        task_id='end'
    )
    # [START howto_operator_sftp_to_gcs_copy_single_file]
    copy_file_from_sftp_to_gcs = SFTPToGCSOperator(
        task_id="file-copy-sftp-to-gcs",
        source_path=os.path.join(TMP_PATH, DIR, OBJECT_SRC_1),
        destination_bucket=BUCKET_SRC,
    )
    # [END howto_operator_sftp_to_gcs_copy_single_file]

    # [START howto_operator_sftp_to_gcs_move_single_file_destination]
    move_file_from_sftp_to_gcs_destination = SFTPToGCSOperator(
        task_id="file-move-sftp-to-gcs-destination",
        source_path=os.path.join(TMP_PATH, DIR, OBJECT_SRC_2),
        destination_bucket=BUCKET_SRC,
        destination_path="destination_dir/destination_filename.bin",
        move_object=True,
    )
    # [END howto_operator_sftp_to_gcs_move_single_file_destination]

    # [START howto_operator_sftp_to_gcs_copy_directory]
    copy_directory_from_sftp_to_gcs = SFTPToGCSOperator(
        task_id="dir-copy-sftp-to-gcs",
        gcp_conn_id="gcs_connection",
        sftp_conn_id="sftp_connection",
        source_path=os.path.join(TMP_PATH, DIR, SUBDIR, "*"),
        destination_bucket=BUCKET_SRC,
        destination_path=SUBDIR,
    )
    # [END howto_operator_sftp_to_gcs_copy_directory]

    # [START howto_operator_sftp_to_gcs_move_specific_files]
    move_specific_files_from_gcs_to_sftp = SFTPToGCSOperator(
        task_id="dir-move-specific-files-sftp-to-gcs",
        source_path=os.path.join(TMP_PATH, DIR, SUBDIR, "*.bin"),
        destination_bucket=BUCKET_SRC,
        destination_path="specific_files/",
        move_object=True,
    )
    # [END howto_operator_sftp_to_gcs_move_specific_files]

    start >> copy_directory_from_sftp_to_gcs >> end
