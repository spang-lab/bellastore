from typing import List, Dict, Tuple
from bellastore.utils.scan import Scan

def get_unique_scans(scans: List[Scan], verbose: False) -> List[Scan]:
    # Check if all scans are at least "hashed"
    for i, scan in enumerate(scans):
        if not scan.state.has_state("hashed"):
            raise ValueError(f"Got a scan at position {i} which is not yet 'hashed'.")

    # Filter for duplicate hashes in input - only insert in the dict, if the hash is not already there
    unique_scans_dict : Dict[str, Scan] = {}

    for scan in scans:
        if scan.hash:
            if scan.hash not in unique_scans_dict:
                unique_scans_dict[scan.hash] = scan
            else:
                if verbose:
                    print(f"Duplicate hash: {scan.hash}")

    unique_scans = list(unique_scans_dict.values())
    return unique_scans


