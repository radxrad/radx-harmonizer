#!/usr/bin/python3
"""
Script for cleaning, validating, and updating the contents of specified directories.

This script is designed to process and validate data files within directories that follow a specific
naming convention. It performs multiple checks on files, including metadata validation, handling of
dictionaries, data type checks, and unit standardization. The script can be used to either reset
the work directory from `preorigcopy` files or rerun validation steps without reinitializing from
`preorigcopy`. Errors encountered during validation are logged into a designated error file.

Key functionalities:
1. Copies files from `preorigcopy` directories to working directories.
2. Performs data validation and cleaning steps, such as checking for missing values, correct formats, and matching data to dictionary files.
3. Generates output files with cleaned and validated data.
4. Optionally resets the work directory or skips certain steps using rerun options.
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
META_DIR = os.path.join(ROOT_DIR, "meta")
HARMONIZED_DICT = os.path.join(ROOT_DIR, "reference/RADx-rad_legacy_dict_2025-06-24.csv")
GLOBAL_HARMONIZED_DICT = os.path.join(ROOT_DIR, "reference/RADx-global_tier1_dict_2025-06-24.csv")
TIER1_HARMONIZED_DICT = os.path.join(ROOT_DIR, "reference/RADx-rad_tier1_dict_2025-06-24.csv")
TIER2_HARMONIZED_DICT = os.path.join(ROOT_DIR, "reference/RADx-rad_tier2_dict_2025-06-24.csv")
ERROR_FILE_NAME = "phase2_errors.csv"


def phase2_checker(include_dirs, exclude_dirs, start, end, reset=False, rerun=False):
    """
    Clean, validate, and update the contents of the specified directories and manage errors.

    Parameters
    ----------
    include_dirs : list of str
        List of directories to include in the check.
    exclude_dirs : list of str
        List of directories to exclude from the check.
    start : int
        Start index for first file (default 0).
    end : int
        End index for first file (default infinite).
    reset : bool, optional
        Flag to indicate if the work directory should be reset (default is False).

    Returns
    -------
    None
    """
    directories = utils.get_directories(include_dirs, exclude_dirs, DATA_DIR)

    for directory in directories:
        preorigcopy_dir = os.path.join(directory, "preorigcopy")

        if not os.path.isdir(preorigcopy_dir):
            print(f"ERROR: Project directory {preorigcopy_dir} does not exist!")
            sys.exit(-1)

        work_dir = os.path.join(directory, "work")

        phase1_error_file = os.path.join(work_dir, "phase1_errors.csv")
        if os.path.exists(phase1_error_file):
            print(f"skipping: {directory} due to Phase I errors")
            continue

        # When rerun is specified, the lock file is ignored
        if not rerun:
            lock_file = os.path.join(work_dir, "lock.txt")
            if os.path.exists(lock_file):
                print(
                    f"skipping {directory}: directory has been locked! "
                    "Remove the lock.txt to make any updates."
                )
                continue

        print(f"checking: {directory} step: ", end="")

        try:
            if reset:
                shutil.rmtree(work_dir)
            os.makedirs(work_dir, exist_ok=True)
        except Exception:
            print(f"skipping {directory}: error resetting/accessing work directory")
            continue

        error_file = os.path.join(work_dir, ERROR_FILE_NAME)

        if os.path.exists(error_file):
            os.remove(error_file)

        if not rerun:
            step1(preorigcopy_dir, start, end, work_dir)
            print("1,", end="")

        error_messages = []

        step2(work_dir, start, end, error_messages)
        if utils.handle_errors_and_continue(error_file, error_messages):
            continue
        print("2", end="")

        step3(work_dir, start, end, error_messages)
        if utils.handle_errors_and_continue(error_file, error_messages):
            continue
        print(",3", end="")

        step4(work_dir, start, end,  error_messages)
        if utils.handle_errors_and_continue(error_file, error_messages):
            continue
        print(",4", end="")

        step5(work_dir, start, end, error_messages)
        if utils.handle_errors_and_continue(error_file, error_messages):
            continue
        print(",5", end="")

        step6(work_dir, start, end)
        print(",6", end="")

        step7(work_dir, start, end, TIER1_HARMONIZED_DICT, TIER2_HARMONIZED_DICT)
        print(",7 - passed")


def step1(preorigcopy_dir, start, end, work_dir):
    """
    Copy preorigcopy files into the work directory.

    Parameters
    ----------
    preorigcopy_dir : str
        Path to the preorigcopy directory.
    start : int
        Start index for first file (default 0).
    end : int
        End index for first file (default infinite).
    work_dir : str
        Path to the work directory.


    Returns
    -------
    None
    """
    dict_files = sorted(glob.glob(os.path.join(preorigcopy_dir, "rad_*_*-*_DICT_*.csv")))

    # Slice the list from 'start' to 'end'
    for dict_file in dict_files[start:end]:
        data_file = dict_file.replace("_DICT_preorigcopy.csv", "_DATA_preorigcopy.csv")
        meta_file = dict_file.replace("_DICT_preorigcopy.csv", "_META_preorigcopy.csv")

        # Combine them into a list
        input_files = [dict_file, data_file, meta_file]
        for input_file in input_files:
            basename = os.path.basename(input_file)
            output_file = os.path.join(work_dir, basename.replace("_preorigcopy.csv", ".csv"))

            if utils.is_newer(input_file, output_file):
                shutil.copyfile(input_file, output_file)


def step2(work_dir, start, end, error_messages):
    """
    Run data checks and data cleanups.

    Parameters
    ----------
    work_dir : str
        Path to the work directory.
    start : int
        Start index for first file (default 0).
    end : int
        End index for first file (default infinite).
    error_messages : list of str
        List to store error messages.

    Returns
    -------
    None
    """
    dict_files = sorted(glob.glob(os.path.join(work_dir, "rad_*_*-*_DICT.csv")))

    # Slice the list from 'start' to 'end'
    for dict_file in dict_files[start:end]:
        data_file = dict_file.replace("_DICT.csv", "_DATA.csv")
        meta_file = dict_file.replace("_DICT.csv", "_META.csv")

        # Combine them into a list
        input_files = [dict_file, data_file, meta_file]
        if len(input_files) == 0:
            print(f"ERROR: Cannot process {work_dir}. No csv files found!")
            sys.exit(-1)

        for input_file in input_files:
            if input_file.endswith("origcopy.csv"):
                os.remove(input_file)
                continue

            error = utils.is_not_utf8_encoded(input_file, error_messages)

            if not error:
                utils.remove_spaces_from_header(input_file)
                utils.remove_empty_rows_cols(input_file, error_messages)
                utils.standardize_units(input_file)


def step3(work_dir, start, end, error_messages):
    """
    Check DICT files for mandatory columns and fix units.

    Parameters
    ----------
    work_dir : str
        Path to the work directory.
    start : int
        Start index for first file (default 0).
    end : int
        End index for first file (default infinite).
    error_messages : list of str
        List to store error messages.

    Returns
    -------
    None
    """
    input_files = sorted(glob.glob(os.path.join(work_dir, "rad_*_*-*_*_DICT.csv")))

    # Slice the list from 'start' to 'end'
    for input_file in input_files[start:end]:
        utils.fix_units(input_file)
        utils.check_dict(input_file, error_messages)


def step4(work_dir, start, end, error_messages):
    """
    Match data fields to data elements in the dictionary files.

    Parameters
    ----------
    work_dir : str
        Path to the work directory.
    start : int
        Start index for first file (default 0).
    end : int
        End index for first file (default infinite).
    error_messages : list of str
        List to store error messages.

    Returns
    -------
    None
    """
    dict_files = sorted(glob.glob(os.path.join(work_dir, "rad_*_*-*_*_DICT.csv")))

    for dict_file in dict_files[start:end]:
        data_file = dict_file.replace("DICT", "DATA")
        utils.data_dict_matcher_new(data_file, dict_file, HARMONIZED_DICT, error_messages)


def step5(work_dir, start, end, error_messages):
    """
    Check for missing values, valid field types, and proper format in the data.

    Parameters
    ----------
    work_dir : str
        Path to the work directory.
    start : int
        Start index for first file (default 0).
    end : int
        End index for first file (default infinite).
    error_messages : list of str
        List to store error messages.

    Returns
    -------
    None
    """
    dict_files = sorted(glob.glob(os.path.join(work_dir, "rad_*_*-*_*_DICT.csv")))

    for dict_file in dict_files[start:end]:
        any_error = False
        error = utils.check_missing_values(dict_file, error_messages)
        any_error = any_error or error

        error = utils.check_field_types(dict_file, error_messages)
        any_error = any_error or error

        error = utils.check_provenance(dict_file, error_messages)
        any_error = any_error or error

        data_file = dict_file.replace("_DICT.csv", "_DATA.csv")
        error = utils.check_data_type(data_file, dict_file, error_messages)
        any_error = any_error or error

        error = utils.check_enums(data_file, dict_file, error_messages)
        any_error = any_error or error

        error = utils.has_study_id(data_file, dict_file, error_messages)
        any_error = any_error or error

        if not any_error:
            meta_file = dict_file.replace("_DICT.csv", "_META.csv")
            meta_output_file = dict_file.replace("_DICT.csv", "_META_origcopy.csv")
            error = utils.update_meta_data(
                meta_file, meta_output_file, META_DIR, data_file, error_messages
            )
            data_output_file = data_file.replace("_DATA.csv", "_DATA_origcopy.csv")
            shutil.copyfile(data_file, data_output_file)


def step6(work_dir, start, end):
    """
    Update dictionary files.

    Parameters
    ----------
    work_dir : str
        Path to the work directory.
    start : int
        Start index for first file (default 0).
    end : int
        End index for first file (default infinite).

    Returns
    -------
    None
    """
    dict_files = sorted(glob.glob(os.path.join(work_dir, "rad_*_*-*_*_DICT.csv")))

    for dict_file in dict_files[start:end]:
        utils.update_dict_file(dict_file, dict_file)


def step7(work_dir, start, end, tier1_dict_file, tier2_dict_file):
    """
    Convert dictionary files to origcopy.

    Parameters
    ----------
    work_dir : str
        Path to the work directory.
    start : int
        Start index for first file (default 0).
    end : int
        End index for first file (default infinite).
    tier1_dict_file : str
        Path to the Tier 1 harmonized dictionary file.
    tier2_dict_file : str
        Path to the Tier 2 harmonized dictionary file.

    Returns
    -------
    None
    """
    dict_files = sorted(glob.glob(os.path.join(work_dir, "rad_*_*-*_*_DICT.csv")))

    for dict_file in dict_files[start:end]:
        dict_output_file = dict_file.replace("_DICT.csv", "_DICT_origcopy.csv")
        utils.convert_dict(dict_file, tier1_dict_file, tier2_dict_file, dict_output_file)


def main(include, exclude, start, end, reset, rerun):
    """
    Main function to execute the phase2_checker with command-line arguments.

    Parameters
    ----------
    include : str or None
        Comma-separated list of projects to include.
    exclude : str or None
        Comma-separated list of projects to exclude.
    start : int
        Start index for first file (default 1).
    end : int
        End index for first file (default infinite).
    reset : bool
        Flag to reset the project files with preorigcopy files.

    Returns
    -------
    None
    """
    print("Phase2: Validate and prepare origcopy files.")

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

    if reset and rerun:
        print("Can not use -reset and -rerun at the same time")
        sys.exit(-1)

    if rerun:
        print()
        print("skipping step 1 for: -rerun")

    if reset:
        reset = True
        print()
        print("WARNING: -reset will remove all files in the work directory!")
        if not utils.confirm_rerun(
            "Are you sure you want to rest the work directory with the files from preorigcopy?"
        ):
            sys.exit(0)
    else:
        reset = False

    print()

    # adjust start index to zero-based indexing
    phase2_checker(include, exclude, start-1, end, reset, rerun)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Check and process preorigcopy files into origcopy files."
    )
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
        help="Reset project using files from preorigcopy.",
    )
    parser.add_argument(
        "-rerun",
        action="store_true",
        help="Rerun phase2 in the work directory without copying files from preorigcopy.",
    )
    parser.add_argument(
        "-start",
        type=int,
        default=1,
        help="Index of the first file to process (default: 1)",
    )
    parser.add_argument(
        "-end",
        type=int,
        default=sys.maxsize,
        help="Index of the last file to process (default: no limit)",
    )

    args = parser.parse_args()
    main(args.include, args.exclude, args.start, args.end, args.reset, args.rerun)
