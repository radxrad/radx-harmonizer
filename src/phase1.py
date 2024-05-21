#!/usr/bin/python3
import os
import glob
import pathlib
import utils


def phase1_checker(preorigcopy_path, work_path):
    """
    Check and validate the contents of directories within a specified path, and manage errors.

    Parameters
    ----------
    preorigcopy_path : str
        Path to the parent directory containing the 'rad_*_*-*' subdirectories to be checked.
    work_path : str
        Path to the directory where the processed subdirectories and error files will be created.

    Returns
    -------
    None

    Notes
    -----
    This function performs the following tasks:
    1. Identifies subdirectories matching the pattern 'rad_*_*-*' within `preorigcopy_path`.
    2. Creates corresponding subdirectories in `work_path`.
    3. Removes any existing error file from previous runs in the target subdirectories.
    4. Checks for missing files and validates metadata files within each subdirectory.
    5. Logs errors in a 'phase1_errors.csv' file within each subdirectory if any issues are found.
    6. Generates an error summary file in `work_path` summarizing all identified errors.

    Examples
    --------
    >>> phase1_checker('/path/to/preorigcopy', '/path/to/work')
    This will process all 'rad_*_*-*' subdirectories in '/path/to/preorigcopy' and
    create corresponding subdirectories in '/path/to/work', checking for errors
    and creating error files as necessary.

    """
    directories = glob.glob(os.path.join(preorigcopy_path, "rad_*_*-*"))
    os.makedirs(work_path, exist_ok=True)
    error_file_name = "phase1_errors.csv"

    for directory in directories:
        # Create rad_*_*-* subdirectories in the work directory
        path = pathlib.PurePath(directory)
        work_dir = os.path.join(work_path, path.name)
        os.makedirs(work_dir, exist_ok=True)

        # Remove error file from previous run
        error_file = os.path.join(work_dir, error_file_name)
        if os.path.exists(error_file):
            os.remove(error_file)

        error = False
        error_messages = []

        # Check for missing files
        error, error_messages = utils.file_is_missing(directory, error_messages)
        if error:
            utils.save_error_file(error_messages, error_file)

        # Check metadata file for correct format and information
        for file in glob.glob(os.path.join(directory, "rad_*_*-*_*_META_preorigcopy.csv")):
            error, error_messaged = utils.check_meta_file(file, error_messages)
            if error:
                utils.save_error_file(error_messages, error_file)

    # Create an error summary file
    utils.create_error_summary(directories, work_path, error_file_name)


if __name__ == "__main__":
    phase1_checker("../preorigcopy", "../work")
    print("Phase 1: Check file: ../work/phase1_errors.csv for errors in preorigcopy files")
