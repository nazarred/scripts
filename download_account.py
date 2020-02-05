"""Download all files from LM account."""
import argparse
import multiprocessing
import os
from pathlib import Path
import logging
import sys
import requests
from requests import codes
import concurrent.futures


host = "https://api.lidarmill.com"
headers = {}

# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
f_handler = logging.FileHandler("downloads.log")
f_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
f_handler.setFormatter(formatter)
logger.addHandler(handler)
logger.addHandler(f_handler)

# Parse args
parser = argparse.ArgumentParser()
parser.add_argument(
    "-u", "--username", required=True, help="LiDARMill username (email).",
)
parser.add_argument(
    "-p", "--password", required=True, help="LiDARMill password.",
)
parser.add_argument(
    "-d",
    "--destination",
    default=os.path.dirname(os.path.realpath(__file__)),
    required=False,
    help="Destination folder, all data will be downloaded there, if not set will be used the same directory where this script is located",
)
parser.add_argument(
    "--skip-cam", action="store_true", help="Skip camera data Artifacts",
)

parser.add_argument(
    "--disable-threads", action="store_true", help="Disable threads for downloading.",
)
parser.add_argument(
    "--debug", action="store_true", help="Run in debug mode",
)
parser.add_argument(
    "-f",
    "--force-download",
    action="store_true",
    help="If set files will by downloaded no meter if it already exists or no, (by default script will skip downloading if file exist)",
)

args = parser.parse_args()

dest_path = Path(args.destination)
skip_cam = args.skip_cam
force_download = args.force_download
username = args.username
password = args.password
disable_threads = args.disable_threads
debug = args.debug
if debug:
    handler.setLevel(logging.DEBUG)
cpu_count = multiprocessing.cpu_count() or 12


logger.debug(f"dest_path {dest_path} skip_cam {skip_cam} force_download {force_download} cpu_count{cpu_count} disable_threads {disable_threads}")


def get_api(url: str) -> dict:
    """Send get request to the API."""
    url = f"{host}{url}"
    response = requests.get(url, headers=headers)
    if response.status_code != codes.OK:
        logger.error(f"GET Request to the backend failed:\nURL: {url}")
        logger.error(
            f"Status code {response.status_code}, Response: {response.json().get('message')}"
        )
        sys.exit(1)
    return response.json()["data"]


def download_file(url: str, full_path: Path, destiny: Path):
    """Download contents of `url` into `dir`.

    The resulting file will be called like the last part of `url`.
    or using the name from content disposition header
    Example: "/foo/bar.png" will result in a file called "bar.png".

    If `dir` doesn't exist, it will be created.
    """
    # Create `dir` if it doesn't exist.
    os.makedirs(str(destiny), exist_ok=True)
    # Load initial headers to get actual length of file.
    initial_headers = requests.head(url).headers
    # Store actual size.
    actual_size = initial_headers.get("Content-Length")
    actual_size = int(actual_size) if actual_size is not None else None
    size_written = 0
    if full_path.is_dir():
        logger.warning(f"Can't download dir {full_path.name}")
        return str(full_path)
    # Continue requesting at the current position until everything has been downloaded.
    with open(str(full_path), "wb") as f:
        while (size_written == 0 and actual_size is None) or (
            actual_size is not None and size_written < actual_size
        ):
            send_headers = {
                "Range": "bytes={begin}-{end}".format(
                    begin=size_written, end=actual_size
                )
            }
            # Do not send headers (with broken range) if the file size could not
            # be determined.
            headers = (
                send_headers if actual_size is not None and actual_size > 0 else None
            )
            resp = requests.get(url, timeout=1200, stream=True, headers=headers,)
            # Treat everything 2xx as okay.
            if resp.status_code < 200 or resp.status_code >= 300:
                logger.info(
                    f"Couldn't download file.\nURL: {url}\n"
                    f"Status Code: {resp.status_code}\n"
                )
                sys.exit(1)

            for chunk in resp.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
                    size_written += len(chunk)

    return str(full_path)


def download_files(files: list, destiny: Path):
    """Download files into destiny folder.

    Skip existing files if it exists and force-downloading is not set
    """
    data_files_download_results = []
    if not disable_threads:
        with concurrent.futures.ThreadPoolExecutor(max_workers=cpu_count) as executor:
            for file_url, file_name in files:
                full_path = destiny / file_name

                logger.debug(f"Downloading {file_url} into file {full_path}")
                # Now we'll get the S3 link from the input `Artifact` and
                # download the data of the link.
                # Firstly check if file exists
                if full_path.is_file():
                    if force_download:
                        # remove file and re-download
                        os.remove(str(full_path))
                    else:
                        # we need to check  if file was completely downloaded
                        # check size for it
                        curreny_size = full_path.stat().st_size
                        initial_headers = requests.head(file_url).headers
                        # Store actual size.
                        actual_size = initial_headers.get("Content-Length")
                        if actual_size and int(actual_size) == curreny_size:
                            logger.debug(f"File {full_path} exists, skip downloading")
                            continue
                        else:
                            # if size does not match remove file and re-download
                            os.remove(str(full_path))
                data_files_download_results.append(
                    executor.submit(download_file, file_url, full_path, destiny)
                )
        for future in data_files_download_results:
            file_name = future.result()
            if file_name:
                logger.debug(f"Download of file {file_name} done.")
            else:
                logger.warning(f"Couldn't download  {file_name}")
    else:
        for file_url, file_name in files:
            logger.debug(f"Downloading {file_url} into file {file_name}")
            # Now we'll get the S3 link from the input `Artifact` and
            # download the data of the link.
            # Firstly check if file exists
            full_path = destiny / file_name
            if full_path.is_file():
                if force_download:
                    # remove file and re-download
                    os.remove(str(full_path))
                else:
                    # we need to check  if file was completely downloaded
                    # check size for it
                    curreny_size = full_path.stat().st_size
                    initial_headers = requests.head(file_url).headers
                    # Store actual size.
                    actual_size = initial_headers.get("Content-Length")
                    if actual_size and int(actual_size) == curreny_size:
                        logger.info(f"File {full_path} exists, skip downloading")
                        continue
                    else:
                        # if size does not match remove file and re-download
                        os.remove(str(full_path))
            data_files_download_results.append(
                download_file(file_url, full_path, destiny)
            )


os.makedirs(str(dest_path), exist_ok=True)

logger.info("Trying to log in.")
data = {"email": username, "password": password}
resp = requests.post(f"{host}/login", json=data)
if resp.status_code == codes.OK:
    logger.info("Got login.")
else:
    logger.fatal("Login failed.")
    sys.exit(1)

headers["Authorization"] = "Bearer {token}".format(token=resp.json()["data"]["token"])
user_id = resp.json()["data"]["user"]["id"]
logger.info("Fetching user`s projects.")

projects = get_api(f"/users/{user_id}/projects")
logger.info(f"Found {len(projects)} projects.")

proj_count = 0
for project in projects:
    project_path = dest_path / project["name"]
    os.makedirs(str(project_path), exist_ok=True)
    logger.info(
        f"Started downloading project: {project['name']} ({proj_count}/{len(projects)}), into {project_path}"
    )
    artifacts = get_api(f"/projects/{project['id']}/artifacts")
    if not skip_cam:
        # exclude potree artifacts
        artifacts = [
            artifact for artifact in artifacts if artifact["artifact_type"] != "potree"
        ]
    if skip_cam:
        # exclude potree and camera artifacts
        artifacts = [
            artifact
            for artifact in artifacts
            if artifact["artifact_type"] not in ["potree", "camera_data"]
        ]

    logger.info(f"Found {len(artifacts)} artifacts")
    art_count = 1
    for artifact in artifacts:
        artifact_path = project_path / artifact["name"]
        os.makedirs(str(artifact_path), exist_ok=True)

        logger.info(
            f"Started downloading artifact: {project['name']} ({art_count}/{len(artifacts)}), into {artifact_path}"
        )
        if artifact["artifact_type"] == "camera_data":
            data_directories = get_api(f"/artifacts/{artifact['id']}/data_directories")
            for data_directory in data_directories:
                detail_data_directory = get_api(
                    f"/data_directories/{data_directory['id']}"
                )
                file_index = detail_data_directory["file_index"]
                files_list = [
                    (file_url, Path(file_url).name) for file_url in file_index
                ]
                download_files(files_list, artifact_path)
        else:
            data_files = get_api(f'/artifacts/{artifact["id"]}/data_files')
            files_list = [
                (data_file["s3_link"], data_file["file_name"])
                for data_file in data_files
            ]
            download_files(files_list, artifact_path)
        art_count += 1
    proj_count += 1
