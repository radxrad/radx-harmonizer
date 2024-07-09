#!/usr/bin/python3
import os
import glob
import pathlib
import shutil
import utils


def phase1_checker(data_path, clean_start=False):
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
    error_file_name = "phase1_errors.csv"

    directories = glob.glob(os.path.join(data_path, "rad_*_*-*"))

    for directory in directories:
        path = pathlib.PurePath(directory)
        preorigcopy_dir = os.path.join(directory, "preorigcopy")
        work_dir = os.path.join(directory, "work")

        if clean_start:
            shutil.rmtree(work_dir, ignore_errors=True)

        os.makedirs(work_dir, exist_ok=True)
        print("checking:", work_dir)
        os.makedirs(work_dir, exist_ok=True)

        # Clean up error file from a previous run
        error_file = os.path.join(work_dir, error_file_name)
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
    utils.create_error_summary(data_path, error_file_name)


if __name__ == "__main__":
    directory = "../data_harmonized"
    error_summary = os.path.join(directory, "phase1_errors.csv")
    error_all = os.path.join(directory, "phase1_errors_all.csv")
    phase1_checker(directory, True)
    print(
        f"Phase 1: Check error summary: {error_summary} and {error_all} for errors in the preorigcopy files."
    )
