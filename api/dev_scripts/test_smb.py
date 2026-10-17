"""Quick manual probe of smbclient methods against the NAS.

Run directly:  python test_smb.py [path]
Uses the same env vars as api/cron.py (NAS_HOST, NAS_USERNAME, NAS_PASSWORD).
"""
import os
import sys
from pathlib import Path

import smbclient
from dotenv import load_dotenv

load_dotenv()

NAS_HOST = os.environ["NAS_HOST"]
NAS_USERNAME = os.environ["NAS_USERNAME"]
NAS_PASSWORD = os.environ["NAS_PASSWORD"]

# default to the share root, or pass a path as the first argument
default_path = str(Path(f"//{NAS_HOST}/")/"Processed_Data"/"derivatives"/"CAT")
path = sys.argv[1] if len(sys.argv) > 1 else default_path


def section(title):
    print(f"\n{'=' * 20} {title} {'=' * 20}")

def enumare_path(path: str, accumulation_list: list):
    section(f"scandir (first 5 entries of {path})")
    for i, entry in enumerate(smbclient.scandir(path)):
        if i >= 5:
            break
        print(f"name:            {entry.name!r}")
        print(f"path:            {entry.path!r}")
        print(f"is_dir():        {entry.is_dir()}")
        print(f"is_file():       {entry.is_file()}")
        print(f"is_symlink():    {entry.is_symlink()}")
        print(f"inode():         {entry.inode()}")
        print(f"last_write_time: {entry._dir_info.last_write_time}")
        print(f"creation_time:   {entry._dir_info.creation_time}")
        print("-" * 50)
        if  entry.is_dir():
            accumulation_list.append(entry.path)


print(f"registering session to {NAS_HOST} ...")
smbclient.register_session(NAS_HOST, username=NAS_USERNAME, password=NAS_PASSWORD)
print(f"probing path: {path}")

section("listdir")
names = smbclient.listdir(path)
print(f"{len(names)} entries, first 10:")
for name in names[:10]:
    print(f"  {name!r}")


path_list = []
enumare_path(path, path_list)

for _path in path_list:
    enumare_path(_path,[])


section("scandir with search pattern 'sub-*' (first 5)")
try:
    for i, entry in enumerate(smbclient.scandir(path, "sub-*")):
        if i >= 5:
            break
        print(f"  {entry.name!r}  dir={entry.is_dir()}  mtime={entry._dir_info.last_write_time}")
except Exception as e:
    print(f"  failed: {type(e).__name__}: {e}")

section("stat on the path itself")
st = smbclient.stat(path)
print(st)
print(f"st_mtime: {st.st_mtime}")

section("path helpers (smbclient.path)")
import smbclient.path as smbpath
print(f"exists:  {smbpath.exists(path)}")
print(f"isdir:   {smbpath.isdir(path)}")
print(f"isfile:  {smbpath.isfile(path)}")
print(f"getmtime:{smbpath.getmtime(path)}")

print("\ndone")
