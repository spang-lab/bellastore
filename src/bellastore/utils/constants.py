from typing import List


scan_extensions         : List[str]     = [".ndpi", ".svs",  ".tif", ".tiff"]        # Used in `scan.py` for simply checking via `endswith` if the file is valid
scan_extensions_glob    : List[str]     = ["*.ndpi", "*.svs", "*.tif", "*.tiff"]    # Used in `scan_database.py` to find all scanner files using globs