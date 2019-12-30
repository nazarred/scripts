
# Get all files in the folder recursively
import glob
from pathlib import Path
folder_path = Path('/mnt/storage/lidarmill/lidarmill-production2/')
all_files = [
        f for f in glob.glob(str(folder_path / "**"), recursive=True) if Path(f).is_file() and not ("data_directories" in f and "out" in f)
    ]

with open('all_files.txt', 'w') as file:
    file.write('\n'.join(all_files))

# with open('all_files.txt', 'r') as file:
#     for l in file.readlines():
#         print(l.strip('\n'))
