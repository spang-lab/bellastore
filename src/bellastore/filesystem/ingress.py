from typing import List, Dict, Tuple
import glob
import os
from .constants import scan_extensions_glob

class Ingress():
    def __init__(self, path):
        """
        The Ingress needs to know about the storage in order to avoid duplicates
        """
        self.path = path
        os.makedirs(path, exist_ok=True)
    
    def get_files(self, verbose = False) -> List[str]:
        """
        Searches all files in a given path for scanner files.
        Files in the 'storage' (`self.path`) will be ignored.

        Args:
            verbose (bool, default = False): print all `PermissionError` files

        Returns:
            scan_files (List[str]): list of paths to all scanner files
        """
        slide_extensions = scan_extensions_glob
        slide_files : List[str] = []
        for ext in slide_extensions:
            try:
                slide_files.extend(glob.glob(f"{self.path}/**/{ext}", recursive=True))
            except PermissionError as e:
                if verbose:
                    print(f"Skipping directory due to permission error: {e}")

        slide_files = [file for file in slide_files]

        return slide_files
    