#!/usr/bin/python3
import os
import sys
import glob
import shutil
import argparse
import utils

# File paths on AWS
# DATA_DIR = "r:\data_harmonized"
# META_DIR = "r:\meta"
# HARMONIZED_DICT = "r:/reference/RADx-rad_HARMONIZED_DICT_2024-08-02.csv"

# File paths local
DATA_DIR = "../data_harmonized"
META_DIR = "../meta"
HARMONIZED_DICT = "../reference/RADx-rad_HARMONIZED_DICT_2024-08-02.csv"

ERROR_FILE_NAME = "phase2_errors.csv"


def phase2_checker(include_dirs, exclude_dirs, reset=False, update=False):
    """
    Validate, clean, and update the contents of directories within the specified paths and manage errors.

    Parameters
    ----------
    include_dirs : list of str
        List of directories to include in the check.
    exclude_dirs : list of str
        List of directories to exclude from the check.
    reset : bool, optional
        Flag to indicate if the work directory should be reset (default is False).
    update : bool, optional
        Flag to indicate if the error summary files should be updated (default is False).

    Returns
    -------
    None

    Notes
    -----
    This function performs the following tasks:
    1. Identifies subdirectories matching the pattern 'rad_*_*-*' within `DATA_DIR`.
    2. Skips directories with Phase 1 errors or locked directories.
    3. Creates or resets the work directory.
    4. Copies preorigcopy files into the work directory.
    5. Runs data checks and cleanups on the copied files.
    6. Matches data fields to data elements in the dictionary files.
    7. Checks for missing values, valid field types, and proper format in the data.
    8. Updates metadata templates and creates updated META files.
    9. Converts and updates dictionary files.
    """
    directories = utils.get_directories(include_dirs, exclude_dirs, DATA_DIR)

    for directory in directories:
        preorigcopy_dir = os.path.join(directory, "preorigcopy")

        # Check if the directory exists
        if not os.path.isdir(preorigcopy_dir):
            print(f"ERROR: Project directory {preorigcopy_dir} does not exist!")
            sys.exit(-1)

        work_dir = os.path.join(directory, "work")

        # Skip and directories with Phase 1 errors
        phase1_error_file = os.path.join(work_dir, "phase1_errors.csv")
        if os.path.exists(phase1_error_file):
            print(f"skipping: {directory} due to Phase I errors")
            continue

        lock_file = os.path.join(work_dir, "lock.txt")
        if os.path.exists(lock_file):
            print(
                f"skipping {directory}, this directory has been locked! Remove the lock.txt to make any updates."
            )
            continue

        print(f"checking: {directory} step: ", end="")

        # Create work directory
        # if reset:
        #     shutil.rmtree(work_dir, ignore_errors=True)
        try:
            if reset:
                shutil.rmtree(work_dir)
            os.makedirs(work_dir, exist_ok=True)
        except Exception:
            print(f"skipping {directory}: error resetting/accessing work directory")
            continue

        # Clean up error file from a previous run
        error_file = os.path.join(work_dir, ERROR_FILE_NAME)

        if os.path.exists(error_file):
            os.remove(error_file)

        # Copy preorigcopy files in to work directory
        step1(preorigcopy_dir, work_dir)
        print("1", end="")

        # Run data checks and data cleanups
        error_messages = []

        step2(work_dir, error_messages)
        print(",2", end="")
        if len(error_messages) > 0:
            utils.save_error_messages(error_file, error_messages)
            print(f" - failed: {len(error_messages)} errors")
            continue
        step3(work_dir, error_messages)
        print(",3", end="")
        if len(error_messages) > 0:
            utils.save_error_messages(error_file, error_messages)
            print(f" - failed: {len(error_messages)} errors")
            continue
        step4(work_dir, error_messages)
        print(",4", end="")
        if len(error_messages) > 0:
            utils.save_error_messages(error_file, error_messages)
            print(f" - failed: {len(error_messages)} errors")
            continue
        step5(work_dir, error_messages)
        print(",5", end="")
        if len(error_messages) > 0:
            utils.save_error_messages(error_file, error_messages)
            print(f" - failed: {len(error_messages)} errors")
            continue
        step6(work_dir)
        print(",6", end="")

        step7(work_dir)
        print(",7 - passed")

    if update:
        # Create error summary files
        utils.create_error_summary(DATA_DIR, ERROR_FILE_NAME)
        # Collect primary keys to be manually checked for consistency
        utils.collect_primary_keys(DATA_DIR)
        # Collect units to be manually checked for consistency
        utils.collect_units(DATA_DIR)


def step1(preorigcopy_dir, work_dir):
    """
    Copy preorigcopy files into the work directory.

    Parameters
    ----------
    preorigcopy_dir : str
        Path to the preorigcopy directory.
    work_dir : str
        Path to the work directory.

    Returns
    -------
    None
    """
    for input_file in glob.glob(os.path.join(preorigcopy_dir, "rad_*_*-*_*.csv")):
        basename = os.path.basename(input_file)
        output_file = os.path.join(
            work_dir, basename.replace("_preorigcopy.csv", ".csv")
        )

        # Proceed only if the input file is newer than the output file or it doesn't exist yet
        if utils.is_newer(input_file, output_file):
            # Copy preorigcopy file to work directory
            shutil.copyfile(input_file, output_file)


def step2(work_dir, error_messages):
    """
    Run data checks and data cleanups.

    Parameters
    ----------
    work_dir : str
        Path to the work directory.
    error_messages : list of str
        List to store error messages.

    Returns
    -------
    None
    """
    input_files = glob.glob(os.path.join(work_dir, "rad_*_*-*_*_*.csv"))
    if len(input_files) == 0:
        print(f"ERROR: Cannot process {work_dir}. No csv files found!")
        sys.exit(-1)

    for input_file in glob.glob(os.path.join(work_dir, "rad_*_*-*_*_*.csv")):
        # Check if file is UTF-8 encoded
        error = utils.is_not_utf8_encoded(input_file, error_messages)

        if not error:
            # Remove space from header to make sure they can be mapped to data elements
            utils.remove_spaces_from_header(input_file)
            # Remove empty rows and columns
            utils.remove_empty_rows_cols(input_file, error_messages)
            # Standardize units in *_unit columns
            utils.standardize_units(input_file)


def step3(work_dir, error_messages):
    """
    Check DICT files for mandatory columns and fix units.

    Parameters
    ----------
    work_dir : str
        Path to the work directory.
    error_messages : list of str
        List to store error messages.

    Returns
    -------
    None
    """
    for input_file in glob.glob(os.path.join(work_dir, "rad_*_*-*_*_DICT.csv")):
        # Some DICT files contain a Units column. Rename it to Unit.
        utils.fix_units(input_file)
        # Check DICT file for mandatory columns
        utils.check_dict(input_file, error_messages)


def step4(work_dir, error_messages):
    """
    Match data fields to data elements in the dictionary files.

    Parameters
    ----------
    work_dir : str
        Path to the work directory.
    error_messages : list of str
        List to store error messages.

    Returns
    -------
    None
    """
    for dict_file in glob.glob(os.path.join(work_dir, "rad_*_*-*_*_DICT.csv")):
        # Match data fields to data elements in the dictionary files
        data_file = dict_file.replace("DICT", "DATA")
        utils.data_dict_matcher_new(
            data_file, dict_file, HARMONIZED_DICT, error_messages
        )


def step5(work_dir, error_messages):
    """
    Check for missing values, valid field types, and proper format in the data.

    Parameters
    ----------
    work_dir : str
        Path to the work directory.
    error_messages : list of str
        List to store error messages.

    Returns
    -------
    None
    """
    for dict_file in glob.glob(os.path.join(work_dir, "rad_*_*-*_*_DICT.csv")):
        any_error = False
        # Check for missing values in mandatory DICT fields
        error = utils.check_missing_values(dict_file, error_messages)
        any_error = any_error or error

        # Check for valid field types in the DICT file
        error = utils.check_field_types(dict_file, error_messages)
        any_error = any_error or error

        # Check provenance column for proper format
        error = utils.check_provenance(dict_file, error_messages)
        any_error = any_error or error

        # Check if the data types in the DATA file match the data types specified in the DICT file
        data_file = dict_file.replace("_DICT.csv", "_DATA.csv")
        error = utils.check_data_type(data_file, dict_file, error_messages)
        any_error = any_error or error

        # Check if the enumerated values used in the DATA file match the enumerations in the DICT file
        error = utils.check_enums(data_file, dict_file, error_messages)
        any_error = any_error or error

        # Check if file that contains minimum CDEs had study_id column.
        error = utils.has_study_id(
            data_file, dict_file, HARMONIZED_DICT, error_messages
        )
        any_error = any_error or error

        if not any_error:
            # Use the metadata templates and combine them with data from the DATA file to create an updated META file
            meta_file = dict_file.replace("_DICT.csv", "_META.csv")
            meta_output_file = dict_file.replace("_DICT.csv", "_META_origcopy.csv")
            error = utils.update_meta_data(
                meta_file,
                meta_output_file,
                META_DIR,
                data_file,
                error_messages,
            )
            # Make a origcopy for the data file
            data_output_file = data_file.replace("_DATA.csv", "_DATA_origcopy.csv")
            shutil.copyfile(data_file, data_output_file)


def step6(work_dir):
    """
    Update dictionary files.

    Parameters
    ----------
    work_dir : str
        Path to the work directory.

    Returns
    -------
    None
    """
    for dict_file in glob.glob(os.path.join(work_dir, "rad_*_*-*_*_DICT.csv")):
        utils.update_dict_file(dict_file, dict_file)


def step7(work_dir):
    """
    Convert dictionary files to origcopy.

    Parameters
    ----------
    work_dir : str
        Path to the work directory.

    Returns
    -------
    None
    """
    for dict_file in glob.glob(os.path.join(work_dir, "rad_*_*-*_*_DICT.csv")):
        dict_output_file = dict_file.replace("_DICT.csv", "_DICT_origcopy.csv")
        utils.convert_dict(dict_file, dict_output_file)


def main(include, exclude, reset, update):
    """
    Main function to execute the phase2_checker with command-line arguments.

    Parameters
    ----------
    include : str or None
        Comma-separated list of projects to include.
    exclude : str or None
        Comma-separated list of projects to exclude.
    reset : bool
        Flag to reset the project files with preorigcopy files.
    update : bool
        Flag to update the phase2_error_summary/details.csv files.

    Returns
    -------
    None
    """
    print("Phase2: Validate and prepare origcopy files.")

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
        if not utils.confirm_rest():
            sys.exit(0)
        print("resetting project files with preorigcopy files")
    else:
        reset = False

    # Convert update flag
    if update:
        update = True
        print("updating phase2_error_summary/details.csv files")
    else:
        update = False

    print()

    # Run phase 2 check
    phase2_checker(include, exclude, reset, update)


if __name__ == "__main__":
    # Create the parser
    parser = argparse.ArgumentParser(
        description="Check and process preorigcopy files into orgicopy files."
    )

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
        "-reset", action="store_true", help="Reset project using files from preorigcopy"
    )
    parser.add_argument(
        "-update",
        action="store_true",
        help="Update phase2_error_summary/details.csv files",
    )

    # Parse the arguments
    args = parser.parse_args()

    # Call the main function with the parsed arguments
    main(args.include, args.exclude, args.reset, args.update)
