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
