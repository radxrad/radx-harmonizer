#!/usr/bin/python3
import os
import sys
import glob
import shutil
import argparse
import utils

# Root directory on AWS
# ROOT_DIR = "r:/"

# Root directory local installation
ROOT_DIR = ".."

# File paths
DATA_DIR = os.path.join(ROOT_DIR, "data_harmonized")
META_DIR = os.path.join(ROOT_DIR, "meta")
HARMONIZED_DICT = os.path.join(
    ROOT_DIR, "reference/RADx-rad_harmonized_dict_2024-08-09.csv"
)
GLOBAL_HARMONIZED_DICT = os.path.join(
    ROOT_DIR, "reference/RADx-global_harmonized_dict_2024-08-12.csv"
)
ERROR_FILE_NAME = "phase3_errors.csv"


def phase3_checker(include_dirs, exclude_dirs, reset=False):
    directories = utils.get_directories(include_dirs, exclude_dirs, DATA_DIR)

    for directory in directories:
        preorigcopy_dir = os.path.join(directory, "work")
        work_dir = os.path.join(directory, "work")
        origcopy_dir = os.path.join(directory, "origcopy")
        transformcopy_dir = os.path.join(directory, "transformcopy")

        # Check if the directory exists
        if not os.path.isdir(work_dir):
            print(f"ERROR: Project directory {work_dir} does not exist!")
            sys.exit(-1)

        # Skip directories with Phase 1 and 2 errors
        phase1_error_file = os.path.join(work_dir, "phase1_errors.csv")
        if os.path.exists(phase1_error_file):
            print(f"skipping: {directory} due to Phase 1 errors")
            continue
        phase2_error_file = os.path.join(work_dir, "phase2_errors.csv")
        if os.path.exists(phase2_error_file):
            print(f"skipping: {directory} due to Phase 2 errors")
            continue

        # Skip directories with a lock.txt file in the transformcopy directory
        lock_file = os.path.join(transformcopy_dir, "lock.txt")
        if os.path.exists(lock_file):
            print(
                f"skipping {directory}, transform copy directory has been locked! "
                "Remove the lock.txt to make any updates."
            )
            continue

        # Clean up error file from a previous run
        error_file = os.path.join(work_dir, ERROR_FILE_NAME)
        if os.path.exists(error_file):
            os.remove(error_file)

        # Reset the origcopy and tranformcopy directories
        try:
            if reset:
                shutil.rmtree(origcopy_dir, ignore_errors=True)
                shutil.rmtree(transformcopy_dir, ignore_errors=True)
        except Exception:
            print(
                f"skipping {directory}: error resetting origcopy and transformcopy directories"
            )
            continue

        print(f"processing: {directory}", end="")

        error = False
        error_messages = []

        # Check for missing files
        postfix = "origcopy"
        error = utils.file_is_missing_in_work_directory(
            work_dir, postfix, error_messages
        )
        if error:
            utils.save_error_file(error_messages, error_file)

        # Check metadata file for correct format and information
        for file in glob.glob(os.path.join(work_dir, "rad_*_*-*_*_META_origcopy.csv")):
            error = utils.check_origcopy_meta_file(file, error_messages)
            if error:
                utils.save_error_file(error_messages, error_file)

        if len(error_messages) > 0:
            utils.save_error_messages(error_file, error_messages)
            print(f" - failed: {len(error_messages)} errors")
            continue

        step0(work_dir, origcopy_dir)

        step1(work_dir, transformcopy_dir)

        step2(work_dir, transformcopy_dir, GLOBAL_HARMONIZED_DICT)

        step3(transformcopy_dir)

        step4(work_dir, origcopy_dir, transformcopy_dir)

        origcopies, transformcopies = utils.final_consistency_check(
            preorigcopy_dir, origcopy_dir, transformcopy_dir, error_messages
        )

        if len(error_messages) > 0:
            utils.save_error_messages(error_file, error_messages)
            print(f" - failed: {len(error_messages)} errors")
            continue

        print(
            f" - {origcopies} origcopy and {transformcopies} transformcopy files created"
        )


def step0(work_dir, origcopy_dir):
    input_files = glob.glob(os.path.join(work_dir, "rad_*_*-*_*_*_origcopy.csv"))
    if len(input_files) == 0:
        print(f"ERROR: Cannot process {work_dir}. No origicopy files found!")
        sys.exit(-1)

    # copy origcopy files from the work directory to the origcopy directory
    os.makedirs(origcopy_dir, exist_ok=True)

    for work_file in glob.glob(os.path.join(work_dir, "rad_*_*-*_*_*_origcopy.csv")):
        origcopy_file_name = os.path.basename(work_file)
        origcopy_file = os.path.join(origcopy_dir, origcopy_file_name)
        shutil.copyfile(work_file, origcopy_file)


def step1(work_dir, transformcopy_dir):
    primary_key = "Id"
    for dict_file in glob.glob(os.path.join(work_dir, "rad_*_*-*_*_DICT_origcopy.csv")):
        if utils.contains_min_cdes(dict_file, primary_key):
            os.makedirs(transformcopy_dir, exist_ok=True)
            # Convert data file to transform copy format
            data_file = dict_file.replace("_DICT_origcopy.csv", "_DATA_origcopy.csv")
            data_file_name = os.path.basename(data_file)
            transformed_data_file = os.path.join(transformcopy_dir, data_file_name)
            transformed_data_file = transformed_data_file.replace(
                "_origcopy.csv", "_transformcopy.csv"
            )
            data = utils.convert_min_to_global_data(data_file)
            data.to_csv(transformed_data_file, index=False)


def step2(work_dir, transformcopy_dir, global_harmonized_dict):
    primary_key = "Id"
    for dict_file in glob.glob(os.path.join(work_dir, "rad_*_*-*_*_DICT_origcopy.csv")):
        if utils.contains_min_cdes(dict_file, primary_key):
            dict_file_name = os.path.basename(dict_file)
            transformed_dict_file = os.path.join(transformcopy_dir, dict_file_name)
            transformed_dict_file = transformed_dict_file.replace(
                "_origcopy.csv", "_transformcopy.csv"
            )
            dictionary = utils.convert_min_to_global_dict(
                dict_file, global_harmonized_dict
            )
            dictionary.to_csv(transformed_dict_file, index=False)


def step3(transformcopy_dir):
    for dict_file in glob.glob(
        os.path.join(transformcopy_dir, "rad_*_*-*_*_DICT_transformcopy.csv")
    ):
        data_file = dict_file.replace(
            "_DICT_transformcopy.csv", "_DATA_transformcopy.csv"
        )
        dictionary = utils.global_data_dict_matcher(data_file, dict_file)
        dictionary.to_csv(dict_file, index=False)


def step4(work_dir, origcopy_dir, transformcopy_dir):
    for meta_work_file in glob.glob(
        os.path.join(work_dir, "rad_*_*-*_*_META_origcopy.csv")
    ):
        # copy the work META file from the work to the origcopy directory
        meta_origcopy_file = os.path.join(
            origcopy_dir, os.path.basename(meta_work_file)
        )
        shutil.copyfile(meta_work_file, meta_origcopy_file)

    for data_file in glob.glob(
        os.path.join(transformcopy_dir, "rad_*_*-*_*_DATA_transformcopy.csv")
    ):
        # For the DATA files in the transformcopy directory, copy the META file
        data_file_name = os.path.basename(data_file)
        meta_file_name = data_file_name.replace(
            "_DATA_transformcopy.csv", "_META_origcopy.csv"
        )
        meta_origcopy_file = os.path.join(origcopy_dir, meta_file_name)
        meta_transformcopy_file = os.path.join(transformcopy_dir, meta_file_name)
        meta_transformcopy_file = meta_transformcopy_file.replace(
            "_META_origcopy.csv", "_META_transformcopy.csv"
        )

        # Replace origcopy prefix in the DATA and DICT names
        # in the META file with the transformcopy prefix
        utils.replace_and_save_text_file(meta_origcopy_file, meta_transformcopy_file)


def main(include, exclude, reset):
    """
    Main function to execute the phase3_checker with command-line arguments.

    Parameters
    ----------
    include : str or None
        Comma-separated list of projects to include.
    exclude : str or None
        Comma-separated list of projects to exclude.
    reset : bool
        Flag to reset the origcopy and transformcopy directories.

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
        print("resetting origcopy and transformcopy directories")
    else:
        reset = False

    print()

    # Run phase 2 check
    phase3_checker(include, exclude, reset)


if __name__ == "__main__":
    # Create the parser
    parser = argparse.ArgumentParser(
        description="Check and generate the origcopy and transformcopy files."
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
        "-reset", action="store_true", help="Reset transformcopy directory"
    )

    # Parse the arguments
    args = parser.parse_args()

    # Call the main function with the parsed arguments
    main(args.include, args.exclude, args.reset)
