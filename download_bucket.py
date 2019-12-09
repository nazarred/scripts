"""Upload all files and folders from input folder recursively."""
import argparse
import os
from pathlib import Path
import logging
import sys
import boto3


logger = logging.getLogger('s3_downloading')
logger.setLevel(logging.DEBUG)

handler = logging.StreamHandler(sys.stdout)
f_handler = logging.FileHandler('s3_upload.log')
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
f_handler.setFormatter(formatter)
logger.addHandler(handler)
logger.addHandler(f_handler)

parser = argparse.ArgumentParser()

parser.add_argument('--f', help='target folder')
parser.add_argument('--s3-access-key', help='Access Key Id', )
parser.add_argument('--s3-secret-key', help='Secret Access Key')
parser.add_argument('--endpoint', help='Endpoint url')
parser.add_argument('--bucket', help='target S3 bucket')
args = parser.parse_args()

folder_path = Path(args.f)
ACCESS_KEY = args.s3_access_key
SECRET_KEY = args.s3_secret_key
BUCKET = args.bucket
ENDPOINT = f"https://{args.endpoint}" if args.endpoint else None
logger.info(f"Path {folder_path}, credentials: {ACCESS_KEY}, {SECRET_KEY}, {BUCKET}, {ENDPOINT}")

session = boto3.session.Session(
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
)
s3 = session.client(
    "s3",
    endpoint_url=ENDPOINT,
    use_ssl=True,
)


def download_dir(local, bucket, client):
    """
    params:
    - prefix: pattern to match in s3
    - local: local path to folder in which to place files
    - bucket: s3 bucket with target contents
    - client: initialized s3 client object
    """
    keys = []
    dirs = []
    next_token = ''
    base_kwargs = {
        'Bucket': bucket,
    }
    while next_token is not None:
        kwargs = base_kwargs.copy()
        if next_token != '':
            kwargs.update({'ContinuationToken': next_token})
        results = client.list_objects_v2(**kwargs)
        contents = results.get('Contents')
        for i in contents:
            k = i.get('Key')
            if k[-1] != '/':
                keys.append(k)
            else:
                dirs.append(k)
        next_token = results.get('NextContinuationToken')
    for d in dirs:
        dest_pathname = os.path.join(local, d)
        if not os.path.exists(os.path.dirname(dest_pathname)):
            logger.info(f"Create empty folder {dest_pathname}")
            os.makedirs(os.path.dirname(dest_pathname))
    for k in keys:
        dest_pathname = os.path.join(local, k)
        if not os.path.exists(os.path.dirname(dest_pathname)):
            logger.info(f"Create folder for key {dest_pathname}")
            os.makedirs(os.path.dirname(dest_pathname))
        logger.info(f"Download file {k}")
        client.download_file(bucket, k, dest_pathname)


if __name__ == "__main__":
    download_dir(folder_path, BUCKET, s3)
