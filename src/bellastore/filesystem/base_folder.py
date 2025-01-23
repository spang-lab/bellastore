import os
from typing import List, Dict, Tuple
import glob
import shutil
from pathlib import Path

from .constants import scan_extensions_glob

class BaseFolder():
    def __init__(self, path):
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
    
    def move_file(self, file_path, target_folder, verbose = False):
        """
        Moves (not copies!) a slide to the target folder.
        A 'slide' might be a simple file or a whole `mrxs` folder.
        
        Args:
            file_path (str): path to the slide file
            target_folder (str): folder where the slide should be moved to
            verbose (bool, default = False): prints moved folders and directories out
        """
        is_mrxs = file_path.endswith(".mrxs")
        
        # Move the slide file itself (for non-`.mrxs` cases, this is enough)
        if verbose:
            print(f"Moving {file_path} to {target_folder}")
        # TODO: Lukas check if this makes sense also for mxrs
        # TODO: this must work
        os.makedirs(target_folder, exist_ok = True)
        dest = shutil.move(file_path, target_folder)
        # important: shutil does not delete the src dir
        p = Path(file_path)
        Path.rmdir(p.parents[0])


        # If it's an `.mrxs` file, we need to also move the directory too (has same name)
        if is_mrxs:
            (mrxs_folder, _) = os.path.splitext(file_path)
            folder_name = os.path.basename(mrxs_folder)
            target = os.path.join(target_folder, folder_name) 

            if verbose:
                print(f"Moving {mrxs_folder} to {target}")
            os.makedirs(target, exist_ok = True)
            dest = shutil.move(mrxs_folder, target) 
            p = Path(mrxs_folder)
            Path.rmdir(p)
        return dest       

    

