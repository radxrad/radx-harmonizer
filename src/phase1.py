#!/usr/bin/python3
"""
Script for validating the contents of `preorigcopy` directories within a specified data structure.

This script is designed to check for missing files, validate metadata, and manage error reporting
for a collection of directories organized according to a specific naming convention (e.g., 'rad_*_*-*').
It can be run on specific sets of directories, with options to include or exclude particular projects,
and a reset option that clears the work directory before execution.

The script performs the following key tasks:
1. Identifies directories that need validation.
2. Validates the presence and format of required files.
3. Logs any detected errors into an error file.
4. Optionally resets the work directory for revalidation.

The script is executed from the command line with arguments for inclusion/exclusion of directories
and an option to reset the working directory.
"""

import argparse
import glob
import os
import shutil
import sys

import utils

# Root directory on AWS
# ROOT_DIR = "r:/"

# Root directory local installation
ROOT_DIR = ".."

# File paths
DATA_DIR = os.path.join(ROOT_DIR, "data_harmonized")
ERROR_FILE_NAME = "phase1_errors.csv"


def phase1_checker(include_dirs, exclude_dirs, reset):
    """
    Validate the contents of preorigcopy directories within the specified paths, and manage errors.

    Parameters
    ----------
    include_dirs : list of str
        List of directories to include in the check.
    exclude_dirs : list of str
        List of directories to exclude from the check.
    reset : bool
        Flag to indicate if the work directory should be reset.

    Returns
    -------
    None

    Notes
    -----
    This function performs the following tasks:
    1. Identifies subdirectories matching the pattern 'rad_*_*-*' within `DATA_DIR`.
    2. Removes any existing error file from previous runs in the target subdirectories.
    3. Checks for missing files and validates metadata files within each preorigcopy subdirectory.
    4. Logs errors in a 'phase1_errors.csv' file within each subdirectory if any issues are found.
    5. Generates an error summary file in `DATA_DIR` summarizing all identified errors.
    """
    directories = utils.get_directories(include_dirs, exclude_dirs, DATA_DIR)

    for directory in directories:
        preorigcopy_dir = os.path.join(directory, "preorigcopy")

        # Check if the directory exists
        if not os.path.isdir(preorigcopy_dir):
            print(f"ERROR: Project directory {preorigcopy_dir} does not exist!")
            sys.exit(-1)

        work_dir = os.path.join(directory, "work")

        lock_file = os.path.join(work_dir, "lock.txt")
        if os.path.exists(lock_file):
            print(
                f"skipping {directory}: directory has been locked! "
                "Remove the lock.txt to make any updates."
            )
            continue

        print(f"checking: {directory}", end="")

        if reset:
            shutil.rmtree(work_dir, ignore_errors=True)

        os.makedirs(work_dir, exist_ok=True)

        # Clean up error file from a previous run
        error_file = os.path.join(work_dir, ERROR_FILE_NAME)
        if os.path.exists(error_file):
            os.remove(error_file)

        error = False
        error_messages = []

        # Check for missing files
        postfix = "preorigcopy"
        error = utils.file_is_missing(preorigcopy_dir, postfix, error_messages)
        if error:
            utils.save_error_file(error_messages, error_file)

        # Check metadata file for correct format and information
        for file in glob.glob(os.path.join(preorigcopy_dir, "rad_*_*-*_*_META_preorigcopy.csv")):
            error = utils.check_meta_file(file, error_messages)
            if error:
                utils.save_error_file(error_messages, error_file)

        if len(error_messages) > 0:
            utils.save_error_messages(error_file, error_messages)
            print(f" - failed: {len(error_messages)} errors")
            continue

        print(" - passed")


def main(include, exclude, reset):
    """
    Main function to execute the phase1_checker with command-line arguments.

    Parameters
    ----------
    include : str or None
        Comma-separated list of projects to include.
    exclude : str or None
        Comma-separated list of projects to exclude.
    reset : bool
        Flag to reset the work directory.

    Returns
    -------
    None
    """

    print("Phase1: Validate preorigcopy files.")

    # Parse command line
    if include and exclude:
        print(
            "Error: The '-include' and '-exclude' arguments cannot be specified at the same time."
        )
        sys.exit(1)
    elif include:
        print(f"including projects: {include}")
    elif exclude:
        print(f"excluding projects: {exclude}")
    else:
        print(
            "Error: Use the '-include' or '-exclude' argument to specify which projects to process."
        )
        sys.exit(1)

    # Convert reset flag
    if reset:
        reset = True
        print()
        print("WARNING: -reset will remove all files in the work directory!")
        if not utils.confirm_rerun("Are you sure you want to rest the work directory?"):
            sys.exit(0)
    else:
        reset = False

    print()

    # Run phase 2 check
    phase1_checker(include, exclude, reset)


if __name__ == "__main__":
    # Create the parser
    parser = argparse.ArgumentParser(description="Validate preorigcopy files.")

    # Add the arguments
    parser.add_argument(
        "-include",
        type=str,
        required=False,
        help="Comma-separated list of projects to include.",
    )
    parser.add_argument(
        "-exclude",
        type=str,
        required=False,
        help="Comma-separated list of projects to exclude.",
    )
    parser.add_argument(
        "-reset",
        action="store_true",
        help="Reset work directory (deletes all files in the work directory!)",
    )

    # Parse the arguments
    args = parser.parse_args()

    # Call the main function with the parsed arguments
    main(args.include, args.exclude, args.reset)
