"""Upload all files and folders from input folder recursively."""
import argparse
import glob
import os
import threading
from pathlib import Path
import logging
import sys

import backoff as backoff
import boto3
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError
from urllib3.exceptions import MaxRetryError

logger = logging.getLogger('s3_uploading')
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

parser.add_argument('--f', help='Path to the folder pretented for uploading')
parser.add_argument('--s3-access-key', help='Access Key Id')
parser.add_argument('--s3-secret-key', help='Secret Access Key')
parser.add_argument('--endpoint', help='Endpoint url')
parser.add_argument('--bucket', help='target S3 bucket')
args = parser.parse_args()

folder_path = Path(args.f)
ACCESS_KEY = args.s3_access_key
SECRET_KEY = args.s3_secret_key
BUCKET = args.bucket
ENDPOINT = f"https://{args.endpoint}"
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


class ProgressPercentage(object):
    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        # To simplify we'll assume this is hooked up
        # to a single filename.
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)" % (
                    self._filename, self._seen_so_far, self._size,
                    percentage))
            sys.stdout.flush()


@backoff.on_exception(
        backoff.expo, (ValueError, MaxRetryError, ConnectionError), max_tries=5
    )
def upload_file(path, key):
    config = TransferConfig(multipart_threshold=1024*25, max_concurrency=16,
                            multipart_chunksize=1024*25, use_threads=True)
    skip = True
    try:
        obj = s3.head_object(Bucket=BUCKET, Key=key)
        if obj['ResponseMetadata']['HTTPStatusCode'] != 200:
            skip = False
    except Exception as e:
        skip = False

    if skip:
        logger.info(f"Object with key {key} exist skipping...")
        return
    s3.upload_file(
        path,
        BUCKET,
        key,
        Config=config,
        Callback=ProgressPercentage(path)
    )


# Get all files in the folder recursively
all_files = [
        Path(f) for f in glob.glob(str(folder_path / "**"), recursive=True) if Path(f).is_file()
    ]
total_files_count = len(all_files)
logger.info(f"Found {total_files_count}")
count = 1
for file_path in all_files:
    related_file_path = file_path.relative_to(folder_path)
    s3_key = str(related_file_path)
    logger.info(f"Uploading file {file_path} with key {s3_key}")
    upload_file(str(file_path), s3_key)
    logger.info(f"Uploaded {count}/{total_files_count}")
    count += 1

('ce64db2e-b845-44af-a6ba-0f908c855eb4', '0679d616-933f-4fea-9747-df57dc8e380a', 'f4b3419a-6b7f-4227-9f15-71960913aa6b', '800c7be6-6128-4588-9bce-0d7087ea15d7', '074a0e98-1cad-4e35-8ef5-4deed3bbe117', 'c631699b-2c59-49d6-861b-49d09a34d223', 'b7212aeb-35bf-403b-a578-17a50cf06112', '753fbbfe-23b4-4264-9690-5b0a2d49d346', '3fe55b3d-5e8f-4c01-9b7f-29183b63e56e', '3928a887-2c26-41a3-afa8-acbbbd44683a', 'c7c1e027-4c6a-4011-8db9-2698cb7512f8', 'f572cac5-d16d-4ecd-83a8-03175601d079')