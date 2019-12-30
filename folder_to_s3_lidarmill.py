"""Upload all files and folders from input folder recursively."""
import argparse
import concurrent.futures
import glob
import os
import threading
from pathlib import Path
import logging
import sys
import psycopg2

import backoff as backoff
import boto3
from boto3.s3.transfer import TransferConfig
from uuid import UUID
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
def upload_file(path, key, cont_disposition, count, total_files_count):
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
    if cont_disposition:
        extra = {"ContentDisposition": cont_disposition}
    else:
        extra = {}
    # s3.upload_file(
    #     path,
    #     BUCKET,
    #     key,
    #     ExtraArgs=extra,
    #     Config=config,
    #     Callback=ProgressPercentage(path)
    # )
    logger.info(f"Uploaded {count}/{total_files_count}")
    return "success"


def get_file_name_from_minio(s3_key):
    # key = path.replace('/mnt/storage/lidarmill/lidarmill-production2/', '')
    session = boto3.session.Session(
        aws_access_key_id='',
        aws_secret_access_key='',
    )
    s3 = session.client(
        "s3",
        endpoint_url='https://storage.lidarmill.com:9001',
        use_ssl=True,
    )
    try:
        cont_desp = s3.get_object(Bucket='lidarmill-production2', Key=s3_key).get('ContentDisposition')
    except Exception:
        logger.warning(f"Can't get object {s3_key}")
    else:
        if cont_desp:
            logger.info(f"Found cont desp {cont_desp} for {s3_key} ")
            return cont_desp.replace('attachment; filename=', '').strip('"').strip(' ').strip("'")
        else:
            logger.warning(f"Can't get ContentDisposition for object {s3_key}")

DATABASE_URI = os.environ.get("DB_URL")


try:
    # connect to DB
    logging.info('Trying connect to DB..')
    conn = psycopg2.connect(DATABASE_URI)
    cursor = conn.cursor()
except Exception as error:
    logging.warning("Can't connect to DB: %s" % error)
    raise Exception

def get_file_name_from_db(s3_key, cursor):
    data_file_id = Path(s3_key).stem
    logger.info(f"Trying to get file name from the db")
    # check if file name is a UUID
    try:
        uuid_obj = UUID(data_file_id, version=4)
    except ValueError:
        logger.info(f"Filename {data_file_id} is not UUID id.")
    else:
        cursor.execute(f"SELECT file_name FROM data_file WHERE id='{data_file_id}'")
        obj = cursor.fetchone()
        if obj:
            logger.info(f"Found name {obj[0]} in the DB")
            return f'attachment; filename="{obj[0]}"'
        else:
            logger.warning(f"Could not find datafile {s3_key}")


# Get all files in the folder recursively
all_files = [
        Path(f) for f in glob.glob(str(folder_path / "**"), recursive=True) if Path(f).is_file() and not ("data_directories" in f and "out" in f)
    ]
total_files_count = len(all_files)
logger.info(f"Found {total_files_count}")
count = 0
res = []
with open('/root/scripts/all_files.txt' 'r') as f:

    with concurrent.futures.ThreadPoolExecutor(
            max_workers=12
    ) as executor:
        for l in f.readlines():
            file_path = Path(l.strip('\n'))
            related_file_path = file_path.relative_to(folder_path)
            s3_key = str(related_file_path)
            if "data_directories" in s3_key and "out" in s3_key:
                logger.info(f"Skipping path {file_path}")
                continue
            file_name = None
            if "data_directories/" not in s3_key:
                file_name = get_file_name_from_db(s3_key, cursor)
                # if not file_name:
                #     file_name = get_file_name_from_minio(s3_key)
            logger.info(f"Uploading file {file_path} with key {s3_key}")
            count += 1
            res.append(
                executor.submit(
                    upload_file,
                    str(file_path),
                    s3_key,
                    file_name,
                    count,
                    total_files_count
                )
            )

for m in res:
    logger.info(m)