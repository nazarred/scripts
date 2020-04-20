"""Upload all files and folders from input folder recursively."""
import argparse
import mimetypes
import multiprocessing
import re
import subprocess
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

cpu_count = 2


parser = argparse.ArgumentParser()

parser.add_argument("--f", help="Path to the folder pretented for uploading")
parser.add_argument("--tmp-dir", help="Path to store temporary archives")
parser.add_argument("--s3-access-key", help="Access Key Id")
parser.add_argument("--s3-secret-key", help="Secret Access Key")
parser.add_argument("--endpoint", help="Endpoint url")
parser.add_argument("--bucket", help="target S3 bucket")
# currently will try to make html files type 'text/html' and set ContentDisposition inline
parser.add_argument(
    "--guess-type", action="store_true", help="Guess MIME type for files",
)
parser.add_argument("--prefix", help="S3 bucket prefix")

args = parser.parse_args()

prefix = args.prefix
parsed_path = Path(args.f)
tmp_folder = Path(args.tmp_dir)

guess_type = args.guess_type
ACCESS_KEY = args.s3_access_key
SECRET_KEY = args.s3_secret_key
BUCKET = args.bucket
ENDPOINT = f"https://{args.endpoint}" if args.endpoint else None

logger = logging.getLogger("s3_uploading")
logger.setLevel(logging.DEBUG)

handler = logging.StreamHandler(sys.stdout)
f_handler = logging.FileHandler(f"s3_upload_7z_{BUCKET}.log")
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
f_handler.setFormatter(formatter)
e_handler = logging.FileHandler(f"s3_upload-7z-errors_{BUCKET}.log")
e_handler.setLevel(logging.ERROR)
e_handler.setFormatter(formatter)
logger.addHandler(e_handler)
logger.addHandler(handler)
logger.addHandler(f_handler)

logger.info(
    f"Path {parsed_path}, credentials: {ACCESS_KEY}, {SECRET_KEY}, {BUCKET}, {ENDPOINT}"
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
                "\r%s  %s / %s  (%.2f%%)"
                % (self._filename, self._seen_so_far, self._size, percentage)
            )
            sys.stdout.flush()

session = boto3.session.Session(
    aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY,
)
client = session.client(
    "s3",
    endpoint_url=ENDPOINT,
    use_ssl=True,
    config=Config(max_pool_connections=200),
)

@backoff.on_exception(
    backoff.expo, (ValueError, MaxRetryError, ConnectionError), max_tries=5
)
def upload_file(path, key, count, total_count, remove_file=False):
    config = TransferConfig(
        multipart_threshold=1024 * 1024,
        max_concurrency=24,
        multipart_chunksize=1024 * 1024,
        use_threads=True,
    )

    message = ""
    try:
        obj = client.head_object(Bucket=BUCKET, Key=key)
        if (
            obj["ResponseMetadata"]["HTTPStatusCode"] == 200
            and obj.get("ContentLength") == Path(path).stat().st_size
        ):
            skip = True
        else:
            skip = False
    except Exception as e:
        skip = False

    if skip:
        message = f"Object with key {key} exist skipping.."
        logger.info(message)
        return key, False, message
    extra_args = {
        "StorageClass": "DEEP_ARCHIVE",
    }
    if guess_type:
        mimetype = mimetypes.guess_type(path)
        if mimetype and mimetype[0]:
            extra_args["ContentType"] = mimetype[0]
            if mimetype[0] == "text/html":
                logger.info(f"Set ContentDisposition: inline for {path}")
                extra_args["ContentDisposition"] = "inline"
    client.upload_file(
        path,
        BUCKET,
        key,
        Config=config,
        # Callback=ProgressPercentage(path),
        ExtraArgs=extra_args,
    )
    logger.info(f"Uploaded ({count}/{total_count}) {path}")
    os.remove(str(path))
    return key, True, message

def get_valid_filename(s):
    """
    Return the given string converted to a string.

    That can be used for a clean
    filename. Remove leading and trailing spaces; convert other spaces to
    underscores; and remove anything that is not an alphanumeric, dash,
    underscore, or dot.
    >>> get_valid_filename("john's portrait in 2004.jpg")
    'johns_portrait_in_2004.jpg'
    """
    s = str(s).strip().replace(' ', '_')
    s = re.sub(r'(?u)[^-\w.]', '', s)
    return re.sub(r'[^\x00-\x7F]+', '_', s)

def main(folder_path: Path, prefix_path: str = None):
    # Get all files in the folder recursively
    if prefix_path:
        final_path = folder_path / prefix_path
    else:
        final_path = folder_path
    total_count = 0
    uploaded_count = 0
    files_to_upload = []

    for file_path in glob.iglob(str(final_path / "**"), recursive=False):
        total_count += 1
        if not Path(file_path).exists():
            continue
        files_to_upload.append(file_path)

    for file_path_to_upload in files_to_upload:
        file_path_to_upload = Path(file_path_to_upload)
        uploaded_count += 1

        if file_path_to_upload.is_dir():
            remove_file = True
            file_path_7z_to_upload = tmp_folder / get_valid_filename(file_path_to_upload.with_suffix('.7z').name)
            related_file_path = file_path_7z_to_upload.relative_to(tmp_folder)
            s3_key = str(related_file_path)
            try:
                obj = client.head_object(Bucket=BUCKET, Key=s3_key)
            except Exception as e:
                pass
            else:
                if obj["ResponseMetadata"]["HTTPStatusCode"] == 200:
                    logger.info(f"Object with key {s3_key} exist skipping..")
                    continue

            if file_path_7z_to_upload.is_file():
                logger.warning(f"Tmp file exists {file_path_7z_to_upload}, will remove it!")
                os.remove(str(file_path_7z_to_upload))
            compress_args = [
                "7z",
                "a",
                "-t7z",
                str(file_path_7z_to_upload),
                "-m0=lzma2",
                "-mx=9",
                "-mfb=64",
                "-md=32m",
                "-ms=on",
                "-r",
                str(file_path_to_upload),
            ]
            logger.info(f"Running {compress_args}")
            try:
                subprocess.call(compress_args)
            except Exception:
                logger.error(f"Failed to zip folder {file_path_to_upload}")
                continue

        elif file_path_to_upload.is_file():
            remove_file = False
            file_path_7z_to_upload = file_path_to_upload
            related_file_path = file_path_7z_to_upload.relative_to(folder_path)
            s3_key = str(related_file_path)
        else:
            logger.error(f"File/ does not exist {file_path_to_upload}")
            continue
        logger.info(
            f"Uploading file {file_path_7z_to_upload} with key {s3_key}"
        )
        upload_file(str(file_path_7z_to_upload), s3_key, uploaded_count, total_count, remove_file)


if __name__ == "__main__":
    main(parsed_path, prefix)
