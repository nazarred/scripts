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
    "950bdbb5-c642-48e0-91c8-f1ab73eabc53",
    "5e4a8199-4938-45a1-9d04-104bc51ad453",
    "9c88aa03-7a42-4d98-a0b7-2d7f6e07e2dc",
    "42269cb3-1a42-4096-8a47-6509e02b91b7",
    "49f8f5cc-1c2e-43f9-a7a2-23a311be8dad",
    "26a37974-429a-4020-afa8-0fcb122fb073",
    "8a87b40d-6f16-48b6-81b8-e22f9dfb536a",
    "cfe6d131-0440-46cb-9c1f-1989e78f64bc",
    "3b940973-f1c5-4da6-92d8-2ce9531b2c01",
    "6e7752d2-33d5-444a-825c-779dbd0fc6f5",
    "424e9ae2-739b-43de-acdb-e3aaf775e5d4",
    "e2620dcb-a2c8-4428-ab33-4881d377cbf4",
    "a9701c3d-e577-46bb-a732-5581d6a6823c",
    "f656865c-00e6-49c9-9748-4a446e61e09a",
    "04bbc2d6-8dc5-4b68-abe0-de35358ba488",
    "bb4e709a-c196-4e25-b93d-7e58008588b2",
    "79d5f61d-1be1-474e-9d4b-7b58414b0bff",
    "13c31dbf-f4de-45b5-9e45-9d6983ea48d8",
    "df208436-f846-48ff-a59d-d52bd8f3e97a",
    "a8fcfe64-3a1c-422f-a3a8-8fa49e6c012f",
    "e9a352e8-8614-4726-acf0-3ce98b9bc050",
    "8db12e7f-da80-4d9e-aeb0-b3cedf8fd521",
    "743057a5-65d7-4e7d-ab48-00f92c9bad4e",
    "7c623c5c-7852-4431-b285-ed52818cce46",
    "02c877d9-a2a3-4af6-b926-e3f7588c4d73",
    "d4fc862b-4f5f-4f04-884e-b4efa1577d77",
    "458fa23c-cfee-404f-a3f1-1f47bcbf87c4",
    "92b1c4ea-4822-41ae-956f-7613987c0b43",
    "63900111-68df-4da0-97db-91c1611f7bae",
    "5f148c73-fe9e-4f23-b78d-3bb1234a2797",
    "bce03898-22a0-4be6-9e53-b0e58382c3eb",
    "958c1c9d-5402-4e44-8ac3-944d321d8e5a",
    "ae6e6b4e-1639-47e3-9a14-5d784e3e6280",
    "1c5e8bd2-8d08-4672-a15f-c26c65264dd1",
    "e90f0d2b-1b34-4ca6-aaf7-d4390984df71",
    "c12104c4-49dc-4381-8194-b0e48f916941",
    "4d730a35-f1a2-4658-a302-997cda8bddeb",
    "c6b8db33-8421-4f3a-b8ed-2e29c468b7a8",
    "84c28346-9411-4cdf-aefb-cbf2ec7b3f84",
    "6beec725-f466-4781-ac72-0c9bd79b1723",
    "1b4222f9-ab04-43a0-bc18-488e6da357c7",
    "5d3945ad-da8a-41f3-a62c-5c06dcf9335b",
    "ef89041d-b5e7-4c39-b85a-ff71851e80ee",
    "b0e3da1e-d71f-4348-855a-40087a5e2b35",
    "6a214065-e3fd-4a24-9670-38816fd1bca6",
    "91df6f1b-0111-4c2b-b312-1b28f3a98105",
    "2d26ac96-e7a9-484f-a01a-21baf1d32cc9",
    "3fec0326-846d-4451-bde8-74a5fb4086f2",
    "c4fbf2bd-f437-4914-bf87-10f37c958af9",
    "626b72f4-2836-4d7b-b4ea-c1fc7fced1cf",
    "198f4954-1a07-4ec2-a0c6-07fe1c6a05c8",
    "1acd5f81-cb92-4212-96b3-b3bc1b7d56e1",
    "8c16298c-e9a3-40f9-b749-bc4ec59c006b",
    "70b293ca-b6ba-414f-8ac9-b30683f234ae",
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
