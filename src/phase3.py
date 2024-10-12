#!/usr/bin/python3
import os
import sys
import glob
import shutil
import argparse
import subprocess

import utils

# Root directory local installation
ROOT_DIR = ".."

# File paths
DATA_DIR = os.path.join(ROOT_DIR, "data_harmonized")
META_DIR = os.path.join(ROOT_DIR, "meta")

METADATA_COMPILER_JAR = os.path.join(ROOT_DIR, "source/radx-rad-metadata-compiler.jar")
METADATA_VALIDATOR_JAR = os.path.join(
    ROOT_DIR, "source/radx-metadata-validator-app-1.0.6.jar"
)
DICTIONARY_VALIDATOR_JAR = os.path.join(
    ROOT_DIR, "source/radx-data-dictionary-validator-app-1.3.4.jar"
)
METADATA_SPEC = os.path.join(ROOT_DIR, "reference/RADxMetadataSpecification.json")
GLOBAL_HARMONIZED_DICT = os.path.join(
    ROOT_DIR, "reference/RADx-global_tier1_dict_2024-10-11.csv"
)
ERROR_FILE_NAME = "phase3_errors.csv"


def phase3_checker(include_dirs, exclude_dirs):
    """
    Executes the phase 3 checks on the specified directories.

    Parameters
    ----------
    include_dirs : list of str
        List of directories to include in the check.
    exclude_dirs : list of str
        List of directories to exclude from the check.

    Returns
    -------
    None
    """
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

        # Clean up files from a previous run
        error_file = os.path.join(work_dir, ERROR_FILE_NAME)
        if os.path.exists(error_file):
            os.remove(error_file)

        # Remove temporary JSON files from a previous run
        json_files = glob.glob(os.path.join(work_dir, "*.json"))
        for json_file in json_files:
            os.remove(json_file)

        # Remove the origcopy and transformcopy directories from a previous run
        shutil.rmtree(origcopy_dir, ignore_errors=True)
        shutil.rmtree(transformcopy_dir, ignore_errors=True)
        os.makedirs(origcopy_dir)
        os.makedirs(transformcopy_dir)

        print(f"processing: {directory}", end="")

        error = False
        error_messages = []

        # Check for missing files
        error = utils.file_is_missing_in_work_directory(
            work_dir, "origcopy", error_messages
        )
        if error:
            utils.save_error_file(error_messages, error_file)

        if utils.handle_errors_and_continue(error_file, error_messages):
            continue

        # Check metadata file for correct format and information
        for file in glob.glob(os.path.join(work_dir, "rad_*_*-*_*_META_origcopy.csv")):
            error = utils.check_origcopy_meta_file(file, error_messages)
            if error:
                utils.save_error_file(error_messages, error_file)

        if utils.handle_errors_and_continue(error_file, error_messages):
            continue

        # Run validation suite on the origcopy files
        if not is_valid_data(work_dir, "origcopy", error_file, error_messages):
            continue

        step0(work_dir, origcopy_dir)
        step1(work_dir, transformcopy_dir)
        step2(work_dir, transformcopy_dir, GLOBAL_HARMONIZED_DICT)
        step3(transformcopy_dir)
        step4(work_dir, transformcopy_dir)

        # Run validation suite on the transformcopy files
        if not is_valid_data(
            transformcopy_dir, "transformcopy", error_file, error_messages
        ):
            continue

        # Remove the metadata .csv file, since it has been replaced by a .json file.
        for meta_file in glob.glob(
            os.path.join(transformcopy_dir, "rad_*_*-*_*_META_transformcopy.csv")
        ):
            os.remove(meta_file)

        origcopies, transformcopies = utils.final_consistency_check(
            preorigcopy_dir, origcopy_dir, transformcopy_dir, error_messages
        )

        if utils.handle_errors_and_continue(error_file, error_messages):
            continue

        print(
            f" - {origcopies} origcopy and {transformcopies} transformcopy files created"
        )


def is_valid_data(work_dir, dir_type, error_file, error_messages):
    """
    Validates the data, metadata, and dictionary for a given directory type.

    Parameters
    ----------
    work_dir : str
        Path to the work directory.
    dir_type : str
        The directory type (either 'origcopy' or 'transformcopy').
    error_file : str
        Path to the error file.
    error_messages : list of str
        List of error messages to append to.

    Returns
    -------
    bool
        True if data validation succeeds, False otherwise.
    """
    compile_metadata(work_dir, dir_type, error_messages)
    if utils.handle_errors_and_continue(error_file, error_messages):
        return False

    validate_dictionary(work_dir, dir_type, error_messages)
    if utils.handle_errors_and_continue(error_file, error_messages):
        return False

    validate_metadata(work_dir, dir_type, error_messages)
    if utils.handle_errors_and_continue(error_file, error_messages):
        return False

    return True


def compile_metadata(work_dir, file_type, error_messages):
    """
    Compiles metadata CSV files into JSON format using an external compiler tool.

    Parameters
    ----------
    work_dir : str
        Path to the working directory.
    file_type : str
        The type of file to compile (e.g., 'origcopy' or 'transformcopy').
    error_messages : list of str
        List of error messages to append to.

    Returns
    -------
    bool
        True if there were any compilation errors, False otherwise.
    """
    any_error = False

    # Compile metadata CSV files to JSON format
    for meta_file in glob.glob(
        os.path.join(work_dir, f"rad_*_*-*_*_META_{file_type}.csv")
    ):
        command = f"java -jar {METADATA_COMPILER_JAR} -c {meta_file} -o {work_dir} -t {METADATA_SPEC}"
        ret = ""
        try:
            ret = subprocess.run(command, capture_output=True, check=True, shell=True)
        except subprocess.CalledProcessError as e:
            message = f"Compilation failed: {meta_file}: {command}: {e.output}: {ret}"
            error = utils.append_error(message, meta_file, error_messages)
            any_error = any_error or error

    return any_error


def validate_dictionary(work_dir, file_type, error_messages):
    """
    Validates dictionary CSV files using an external validator tool.

    Parameters
    ----------
    work_dir : str
        Path to the working directory.
    file_type : str
        The type of file to validate (e.g., 'origcopy' or 'transformcopy').
    error_messages : list of str
        List of error messages to append to.

    Returns
    -------
    bool
        True if there were any validation errors, False otherwise.
    """
    any_error = False

    # Validate dictionary CSV file
    for dict_file in glob.glob(
        os.path.join(work_dir, f"rad_*_*-*_*_DICT_{file_type}.csv")
    ):
        command = f"java -jar {DICTIONARY_VALIDATOR_JAR} --in={dict_file}"
        ret = ""
        try:
            ret = subprocess.run(command, capture_output=True, check=True, shell=True)
        except subprocess.CalledProcessError as e:
            message = f"Dictionary validation failed: {dict_file}: {command}: {e.output}: {ret}"
            error = utils.append_error(message, dict_file, error_messages)
            any_error = any_error or error

    return any_error


def validate_metadata(work_dir, file_type, error_messages):
    """
    Validates metadata files using an external validator tool.

    Parameters
    ----------
    work_dir : str
        Path to the working directory.
    file_type : str
        The type of file to validate (e.g., 'origcopy' or 'transformcopy').
    error_messages : list of str
        List of error messages to append to.

    Returns
    -------
    bool
        True if there were any validation errors, False otherwise.
    """
    any_error = False

    # Validate metadata files
    for dict_file in glob.glob(
        os.path.join(work_dir, f"rad_*_*-*_*_DICT_{file_type}.csv")
    ):
        data_file = dict_file.replace(f"DICT_{file_type}.csv", f"DATA_{file_type}.csv")
        meta_file = dict_file.replace(f"DICT_{file_type}.csv", f"META_{file_type}.json")
        command = (
            f"java -jar {METADATA_VALIDATOR_JAR} --data={data_file} "
            f"--dict={dict_file} --instance={meta_file} --template={METADATA_SPEC}"
        )
        ret = ""
        try:
            ret = subprocess.run(command, capture_output=True, check=True, shell=True)
        except subprocess.CalledProcessError as e:
            message = (
                f"Metadata validation failed: {data_file}: {command}: {e.output}: {ret}"
            )
            error = utils.append_error(message, dict_file, error_messages)
            any_error = any_error or error

    return any_error


def step0(work_dir, origcopy_dir):
    """
    Prepares origcopy files by copying them from the working directory to the origcopy directory.

    Parameters
    ----------
    work_dir : str
        Path to the working directory.
    origcopy_dir : str
        Path to the origcopy directory.

    Returns
    -------
    None
    """
    input_files = glob.glob(os.path.join(work_dir, "rad_*_*-*_*_*_origcopy.csv"))
    if len(input_files) == 0:
        print(f"ERROR: Cannot process {work_dir}. No origcopy files found!")
        sys.exit(-1)

    # Copy origcopy files from the work directory to the origcopy directory
    os.makedirs(origcopy_dir, exist_ok=True)

    for work_file in glob.glob(os.path.join(work_dir, "rad_*_*-*_*_*_origcopy.csv")):
        origcopy_file_name = os.path.basename(work_file)
        # Use JSON file instead of the CSV file for metadata
        if work_file.endswith("META_origcopy.csv"):
            work_file = work_file.replace("META_origcopy.csv", "META_origcopy.json")
        if origcopy_file_name.endswith("META_origcopy.csv"):
            origcopy_file_name = origcopy_file_name.replace(
                "META_origcopy.csv", "META_origcopy.json"
            )

        origcopy_file = os.path.join(origcopy_dir, origcopy_file_name)
        shutil.copyfile(work_file, origcopy_file)


def step1(work_dir, transformcopy_dir):
    """
    Converts origcopy data files into transformcopy format.

    Parameters
    ----------
    work_dir : str
        Path to the working directory.
    transformcopy_dir : str
        Path to the transformcopy directory.

    Returns
    -------
    None
    """
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
    """
    Converts origcopy dictionary files into global transformcopy format.

    Parameters
    ----------
    work_dir : str
        Path to the working directory.
    transformcopy_dir : str
        Path to the transformcopy directory.
    global_harmonized_dict : str
        Path to the global harmonized dictionary.

    Returns
    -------
    None
    """
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
    """
    Matches the global dictionary to the transformcopy data files.

    Parameters
    ----------
    transformcopy_dir : str
        Path to the transformcopy directory.

    Returns
    -------
    None
    """
    for dict_file in glob.glob(
        os.path.join(transformcopy_dir, "rad_*_*-*_*_DICT_transformcopy.csv")
    ):
        data_file = dict_file.replace(
            "_DICT_transformcopy.csv", "_DATA_transformcopy.csv"
        )
        dictionary = utils.global_data_dict_matcher(data_file, dict_file)
        dictionary.to_csv(dict_file, index=False)


def step4(work_dir, transformcopy_dir):
    """
    Copies and updates metadata for transformcopy files.

    Parameters
    ----------
    work_dir : str
        Path to the working directory.
    transformcopy_dir : str
        Path to the transformcopy directory.

    Returns
    -------
    None
    """
    for data_file in glob.glob(
        os.path.join(transformcopy_dir, "rad_*_*-*_*_DATA_transformcopy.csv")
    ):
        # For the DATA files in the transformcopy directory, copy the META file
        data_file_name = os.path.basename(data_file)
        meta_file_name = data_file_name.replace(
            "_DATA_transformcopy.csv", "_META_origcopy.csv"
        )
        meta_origcopy_file = os.path.join(work_dir, meta_file_name)
        meta_transformcopy_file = os.path.join(transformcopy_dir, meta_file_name)
        meta_transformcopy_file = meta_transformcopy_file.replace(
            "_META_origcopy.csv", "_META_transformcopy.csv"
        )

        # Replace origcopy prefix in the DATA and DICT names
        # in the META file with the transformcopy prefix
        utils.replace_and_save_text_file(meta_origcopy_file, meta_transformcopy_file)

        # Update the sha256 digest
        utils.update_sha256_digest(data_file, meta_transformcopy_file)


def main(include, exclude):
    """
    Main function to execute the phase3_checker with command-line arguments.

    Parameters
    ----------
    include : str or None
        Comma-separated list of projects to include.
    exclude : str or None
        Comma-separated list of projects to exclude.

    Returns
    -------
    None
    """
    print("Phase3: Validate and create origcopy and transformcopy files.")

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

    print()

    # Run phase 2 check
    phase3_checker(include, exclude)


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

    # Parse the arguments
    args = parser.parse_args()

    # Call the main function with the parsed arguments
    main(args.include, args.exclude)
