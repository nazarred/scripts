"""Upload all files and folders from input folder recursively."""
import argparse
import multiprocessing
import concurrent.futures
import glob
import os
import threading
from pathlib import Path
import logging
import sys
from botocore.config import Config

import backoff as backoff
import boto3
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError
from urllib3.exceptions import MaxRetryError

cpu_count = 12


logger = logging.getLogger('s3_uploading')
logger.setLevel(logging.DEBUG)

handler = logging.StreamHandler(sys.stdout)
f_handler = logging.FileHandler('s3_upload.log')
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
f_handler.setFormatter(formatter)
e_handler = logging.FileHandler("downloads-errors.log")
e_handler.setLevel(logging.ERROR)
e_handler.setFormatter(formatter)
logger.addHandler(e_handler)
logger.addHandler(handler)
logger.addHandler(f_handler)

parser = argparse.ArgumentParser()

parser.add_argument('--f', help='Path to the folder pretented for uploading')
parser.add_argument('--s3-access-key', help='Access Key Id')
parser.add_argument('--s3-secret-key', help='Secret Access Key')
parser.add_argument('--endpoint', help='Endpoint url')
parser.add_argument('--bucket', help='target S3 bucket')
args = parser.parse_args()

parsed_path = Path(args.f)
ACCESS_KEY = args.s3_access_key
SECRET_KEY = args.s3_secret_key
BUCKET = args.bucket
ENDPOINT = f"https://{args.endpoint}"
logger.info(f"Path {parsed_path}, credentials: {ACCESS_KEY}, {SECRET_KEY}, {BUCKET}, {ENDPOINT}")


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
def upload_file(path, key, count, total):
    config = TransferConfig(multipart_threshold=1024*1024, max_concurrency=10,
                            multipart_chunksize=1024*1024, use_threads=True)
    session = boto3.session.Session(
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
    )
    client = session.client(
        "s3",
        endpoint_url=ENDPOINT,
        use_ssl=True,
        config=Config(max_pool_connections=200),

    )
    try:
        obj = client.head_object(Bucket=BUCKET, Key=key)
        if obj['ResponseMetadata']['HTTPStatusCode'] == 200 and\
                obj.get('ContentLength') == Path(path).stat().st_size:
            skip = True
        else:
            skip = False
    except Exception as e:
        skip = False

    if skip:
        logger.info(f"Object with key {key} exist skipping...")
        return key, False
    client.upload_file(
        path,
        BUCKET,
        key,
        Config=config,
        # Callback=ProgressPercentage(path)
    )
    logger.info(f"Uploaded {count}/{total} {path}")
    return key, True


def main(folder_path: Path):
    # Get all files in the folder recursively
    all_files = [
            Path(f) for f in glob.glob(str(folder_path / "**"), recursive=True) if Path(f).is_file()
        ]
    total_files_count = len(all_files)
    logger.info(f"Found {total_files_count}")
    count = 1
    data_files_upload_results = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=cpu_count) as executor:
        for file_path in all_files:
            related_file_path = file_path.relative_to(folder_path)
            s3_key = str(related_file_path)
            logger.info(f"Uploading file {file_path} with key {s3_key}")
            data_files_upload_results.append(
                executor.submit(upload_file, str(file_path), s3_key, count, total_files_count)
            )
            count += 1
    for future in data_files_upload_results:
        key, uploaded = future.result()
        if not uploaded:
            logger.error(f"Failed to upload file {key}")
        else:
            logger.info(f"Successfully uploaded {key}")


if __name__ == "__main__":
    main(parsed_path)
