import re
import sys
import argparse
import logging
import shutil
from pathlib import Path

logger = logging.getLogger('move_files')
logger.setLevel(logging.DEBUG)

handler = logging.StreamHandler(sys.stdout)
f_handler = logging.FileHandler('move_files.log')
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
f_handler.setFormatter(formatter)
logger.addHandler(handler)
logger.addHandler(f_handler)

parser = argparse.ArgumentParser()

parser.add_argument('--i', help='Input Folder')
parser.add_argument('--o', help='Output folder')
args = parser.parse_args()

input_path = args.i
output_path = args.o

projects_list = (
    'ce64db2e-b845-44af-a6ba-0f908c855eb4',
    '0679d616-933f-4fea-9747-df57dc8e380a',
    'f4b3419a-6b7f-4227-9f15-71960913aa6b',
    '800c7be6-6128-4588-9bce-0d7087ea15d7',
    '074a0e98-1cad-4e35-8ef5-4deed3bbe117',
    'c631699b-2c59-49d6-861b-49d09a34d223',
    'b7212aeb-35bf-403b-a578-17a50cf06112',
    '753fbbfe-23b4-4264-9690-5b0a2d49d346',
    '3fe55b3d-5e8f-4c01-9b7f-29183b63e56e',
    '3928a887-2c26-41a3-afa8-acbbbd44683a',
    'c7c1e027-4c6a-4011-8db9-2698cb7512f8',
    'f572cac5-d16d-4ecd-83a8-03175601d079'
)


def move_files_and_dirs(input_path, target_path):
    input_path = Path(str(input_path))
    target_path = Path(str(target_path))
    regex_expr = r".*({regex}).*".format(regex="|".join(projects_list))
    for path in input_path.iterdir():
        if not re.match(regex_expr, str(path), flags=re.IGNORECASE):
            logger.info(f"Skiping {path}")
            continue
        relative_path = path.relative_to(input_path)
        final_target_path = target_path / relative_path
        logger.info(f"Moving {relative_path} to {final_target_path}")
        try:
            shutil.move(str(path), str(target_path))
            logger.info('Moved')
        except FileNotFoundError:
            logger.info('file not found')


if __name__ == "__main__":
    logger.info(f"From {input_path} to {output_path}")
    move_files_and_dirs(input_path, output_path)
