import re
import os
import smbclient
from src.feng.config.config import FILEIP05, FILEIP_USERNAME, FILEIP_PASSWORD

def get_file_type_and_region(file_path):
    """
    Extract file type and region from Bloomberg filename.

    Args:
        file_path (str): Path to Bloomberg file

    Returns:
        tuple: (region, file_type) where:
            - region (str): Region code (e.g., 'namr', 'asia1')
            - file_type (str): File type code (e.g., 'cf', 'is')
    """
    # Extract the filename from the path
    filename = os.path.basename(file_path)

    # Define the regex pattern to match Bloomberg filenames
    # This pattern handles both regular region_filetype and special cases like namr_sard_is
    pattern = r'fundamentals_([a-z0-9]+)_([a-z0-9]+)(?:_([a-z0-9]+))?\.out\.\d+'
    match = re.search(pattern, filename)

    if match:
        region_code = match.group(1)

        if match.group(3):
            # If there's a third group, it's the region and the second and third groups form the file type
            file_type_code = f"{match.group(2)}_{match.group(3)}"
        else:
            # Normal case: first group is region, second is file type
            file_type_code = match.group(2)
        return region_code, file_type_code

    if match is None:
        pattern = re.compile(
            r'fundamentals_'                     # literal prefix
            r'(?P<region>[a-z0-9]+)_'            # ← region
            r'(?:'                               #   ┬─── branch
                r'sard_(?P<sardft>[a-z0-9]+)'    #   |    sard_<ft>
              r'|'                               #   |
                r'(?P<ft>[a-z0-9]+)'             #   └─── plain <ft>
            r')'                                 #  ┴
            r'(?:_history)?'                     # optional "_history"
            r'\.out(?:\.\d+)?$',                 # ".out" or ".out.<n>"
            re.IGNORECASE
        )
        match = re.search(pattern, filename)
        if match is None:
            return "Unknown", "Unknown"
        region = match.group('region')  # 'namr'
        filetype = f"sard_{match['sardft']}" if match['sardft'] else match['ft']
        return region, filetype

    return "Unknown", "Unknown"

def get_smbclient():
    """
    This functions is to get smbclient, mainly for obtaining data from Windows OS
    :return: smb client
    """
    smbclient.register_session(
        FILEIP05,
        FILEIP_USERNAME,
        FILEIP_PASSWORD
    )
    return smbclient
