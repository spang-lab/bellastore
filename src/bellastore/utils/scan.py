import os
import glob
import hashlib
import base64
import shutil
from typing import List

from .constants import scan_extensions


class Scan():
    """
    Class representing a single scan.

    Init:
    -----
        **path** _str_ : full path to the scan

    Attributes:
        path (str): the full path to the scan, including the filename and ending
        filename (str): just the filename, without the extension
        hash (str | None, default = None): the hash of the file, empty per default

    Methods
    -------
    <p>
        **get_filename**<em>(self, path) -> str</em><br>constructs the filename out of the full path<br>
        **is_valid**<em>(self) -> bool</em><br>checks if a given file file has a scanner-file ending<br>
        **hash_scan**<em>(self) -> str | None</em><br>creates an unique hash for a scan using `sha256`
    </p>
    """
    def __init__(self, path : str):
        self.path = path
        self.scanname = self.get_scanname(path = self.path)
        self.filename = self.get_filename(path = self.path)
        self.hash : None | str = None


    def get_scanname(self, path : str) -> str:
        """
        Returns the filename without the extension of a given path.

        Args:
            path (str): full path to the file

        Returns:
            filename (str): the filename without extension
        """
        return os.path.splitext(os.path.basename(path))[0]
    # TODO: extend this for mxrs, currently this only works for files
    def move(self, target_dir):
        '''
        Moves a scan file into the target directory
        '''
        try:
            source_path = self.path
            # It is crucial to create the target dir before shutil.move
            os.makedirs(target_dir, exist_ok = True)
            shutil.move(self.path, target_dir)
            self.path = os.path.join(target_dir, self.filename)
            print(f"Successfully moved {source_path} into {self.path}")
        except Exception as e:
            raise RuntimeError(f"File can not be moved from {self.path} into {self.path} due to: {e}")

    
    def get_filename(self, path : str) -> str:
        """
        Returns the filename without the extension of a given path.

        Args:
            path (str): full path to the file

        Returns:
            filename (str): the filename without extension
        """
        return os.path.basename(path)


    def is_valid(self) -> bool:
        """
        Checks if the slide has a scanner-file ending.
        These are `.mrxs`, `.svs`, `.ndpi` and `.tif`.

        Returns:
            is_valid (bool): bool indicating if the scan's path has a valid ending
        """
        #scan_extensions = [".ndpi", ".svs", ".mrxs", ".tif"] # CARE
        return any(self.path.endswith(ending) for ending in scan_extensions)

    # TODO: Lukas integrate and test for mxrs, more modular would also be nicer, e.g. _create_raw_hash,_hash_mxrs ...
    def hash_scan(self) -> str | None:
        """
        Creates an url-safe, base64, utf-8 encoded hash for a scan.
        For scans that consist of more than a single file it hashes the whole directory.
        For non-hashable files it will return `None`.

        Returns:
            hash (str | None): the scan's hash (if non-hashable this is `None`)
        """
        def hash_file(path) -> bytes:
            """
            Hashes a single file in chunks of 64kb
            """
            hash = hashlib.sha256()
            if not os.path.isfile(path):
                raise ValueError(f"{path} is not a file")
            with open(path, "rb") as f:
                while True:
                    data = f.read(65536)
                    if not data:
                        break
                    hash.update(data)
            return hash.digest()

        # Check if the slide even is hashable
        if not self.is_valid():
            print(f"Slide is not valid and thus can not be hashed.")
            return None
        is_mrxs = self.path.endswith(".mrxs")
        if not is_mrxs:
            raw_hash = hash_file(self.path)
            hash = base64.urlsafe_b64encode(raw_hash).decode("utf-8")
            self.hash = hash
            return base64.urlsafe_b64encode(raw_hash).decode("utf-8")

        # For `.mrxs` files check if they are structured correctly
        (mrxs_folder, _) = os.path.splitext(self.path)
        if not os.path.isdir(mrxs_folder):
            print(f"Invalid slide {self.path}: {mrxs_folder} is not a directory")
            return None

        # For `.mrxs` files hash all the files one by one
        hash = hashlib.sha256()
        for root, _, files in os.walk(mrxs_folder):
            for file in files:
                file_path = os.path.join(root, file)
                raw_hash = hash_file(file_path)
                hash.update(raw_hash)
        hash.update(hash_file(self.path))
        hash = base64.urlsafe_b64encode(hash.digest()).decode("utf-8")
        self.hash = hash
        return hash




    def __repr__(self) -> str:
        return f"\nCurrent Path: {self.path}\nCurrent Filename: {self.filename}"