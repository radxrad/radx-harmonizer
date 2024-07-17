#!/usr/bin/python3
import os
import glob
import shutil
import utils


# File paths on AWS
# DATA_DIR = "r:\data_harmonized"

# File paths local
DATA_DIR = "../data_harmonized"

ERROR_FILE_NAME = "phase1_errors.csv"


def phase1_checker(reset=False):
    """
    Check and validate the contents of directories within a specified path, and manage errors.

    Parameters
    ----------
    data_path : str
        Path to the parent directory containing the 'rad_*_*-*' subdirectories to be checked.

    Returns
    -------
    None

    Notes
    -----
    This function performs the following tasks:
    1. Identifies subdirectories matching the pattern 'rad_*_*-*' within `data_path`.
    3. Removes any existing error file from previous runs in the target subdirectories.
    4. Checks for missing files and validates metadata files within each preorigcopy subdirectory.
    5. Logs errors in a 'phase1_errors.csv' file within each subdirectory if any issues are found.
    6. Generates an error summary file in `data_path` summarizing all identified errors.

    Examples
    --------
    >>> phase1_checker('/path/to/data_harmonized')
    This will process all 'rad_*_*-*' subdirectories and create corresponding work directories
    check for errors and create error files as necessary.

    """
    directories = glob.glob(os.path.join(DATA_DIR, "rad_*_*-*"))

    for directory in directories:
        print("checking:", directory)
        preorigcopy_dir = os.path.join(directory, "preorigcopy")
        work_dir = os.path.join(directory, "work")

        if reset:
            shutil.rmtree(work_dir, ignore_errors=True)

        os.makedirs(work_dir, exist_ok=True)
        os.makedirs(work_dir, exist_ok=True)

        # Clean up error file from a previous run
        error_file = os.path.join(work_dir, ERROR_FILE_NAME)
        if os.path.exists(error_file):
            os.remove(error_file)

        error = False
        error_messages = []

        # Check for missing files
        error = utils.file_is_missing(preorigcopy_dir, error_messages)
        if error:
            utils.save_error_file(error_messages, error_file)

        # Check metadata file for correct format and information
        for file in glob.glob(
            os.path.join(preorigcopy_dir, "rad_*_*-*_*_META_preorigcopy.csv")
        ):
            error = utils.check_meta_file(file, error_messages)
            if error:
                utils.save_error_file(error_messages, error_file)

    # Create error summary files
    utils.create_error_summary(DATA_DIR, ERROR_FILE_NAME)


if __name__ == "__main__":
    phase1_checker(True)

    # error_summary = os.path.join(
    #     DATA_DIR, ERROR_FILE_NAME.replace(".csv", "_summary.csv")
    # )
    # error_details = os.path.join(
    #     DATA_DIR, ERROR_FILE_NAME.replace(".csv", "_details.csv")
    # )

    # print()
    # print(f"Error summary: {error_summary}")
    # print(f"Error details: {error_details}")
