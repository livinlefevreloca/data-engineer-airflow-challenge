import boto3
import os
import logging
import shutil

from datetime import datetime


logging.basicConfig(level='INFO')


def upload(**context):
    """
    A function to upload a directory of csvs to S3.
    The csv's are sorted into directories by their source and
    labeled by the date the were created on

    params
        :context(dict) -> a keyword mapping of task information.

    possible_failures:
        If an error is raised during the uploading of the files to s3 the error
        is caught, logged and raised to trigger the DAG to retry the task.
    """
    try:
        csv_dir = context["ti"].xcom_pull(task_ids="tabularize")
        aws_key = os.environ["AWS_KEY"]
        secret_key = os.environ["AWS_SECRET_KEY"]
        bucket_name = os.environ["S3_BUCKET1"] if 'source' in csv_dir \
            else os.environ["S3_BUCKET2"]

        s3 = boto3.client("s3",
                          aws_access_key_id=aws_key,
                          aws_secret_access_key=secret_key)
        # get named of temporary directory the csvs are stored in return by the
        # previous task.

        date_string = datetime.now().strftime("%Y-%m-%d")
        s3_name = date_string + "_top_headlines.csv"

        logging.info("uploading stored files to s3")

        for file in os.listdir(csv_dir):
            file_name = os.path.join(csv_dir, file)
            s3_dir = file.split(".")[0]
            s3_key = os.path.join(s3_dir, s3_name)
            s3.upload_file(Filename=file_name,
                           Bucket=bucket_name,
                           Key=s3_key)

            logging.info("Uploaded {} to s3 key {}".format(file, s3_key))
    except Exception as e:
        logging.exception(e)
        raise
    # remove temporary csv directory
    shutil.rmtree(csv_dir)
    logging.info("Removed temporary csv directory: {}".format(csv_dir))
