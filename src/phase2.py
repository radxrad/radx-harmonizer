#!/usr/bin/python3
import os
import sys
import glob
import pathlib
import shutil
import utils
import argparse

# File paths on AWS
# DATA_DIR = "r:\data_harmonized"
# META_DIR = "r:\meta"]
# HARMONIZED_DICT = "r:\reference\RADx-rad_HARMONIZED_DICT_2024-07-12.csv"

# File paths local
DATA_DIR = "../data_harmonized"
META_DIR = "../meta"
HARMONIZED_DICT = "../reference/RADx-rad_HARMONIZED_DICT_2024-07-12.csv"

ERROR_FILE_NAME = "phase2_errors.csv"

def phase2_checker(DATA_DIR, include_dirs, exclude_dirs, META_DIR, HARMONIZED_DICT, reset=False):
    directories = get_directories(DATA_DIR, include_dirs, exclude_dirs)

    for directory in directories:
        path = pathlib.PurePath(directory)
        preorigcopy_dir = os.path.join(directory, "preorigcopy")
        work_dir = os.path.join(directory, "work")

        # Skip and directories with Phase 1 errors
        phase1_error_file = "phase1_errors.csv"
        phase1_error_file = os.path.join(work_dir, phase1_error_file)
        if os.path.exists(phase1_error_file):
            print(f"skipping: {directory} due to Phase I errors")
            continue

        print(f"checking: {directory}")
        
        # Create work directory
        if reset:
            shutil.rmtree(work_dir, ignore_errors=True)
        os.makedirs(work_dir, exist_ok=True)

        # Clean up error file from a previous run
        error_file = os.path.join(work_dir, ERROR_FILE_NAME)

        if os.path.exists(error_file):
            os.remove(error_file)

        # Copy preorigcopy files in to work directory
        step1(preorigcopy_dir, work_dir)

        # Run data checks
        error_messages = []
        step2(work_dir, error_file, error_messages)
        if len(error_messages) > 0:
            utils.save_error_messages(error_file, error_messages)
            continue
        step3(work_dir, error_file, error_messages)
        if len(error_messages) > 0:
            utils.save_error_messages(error_file, error_messages)
            continue
        step4(work_dir, error_file, error_messages, HARMONIZED_DICT)
        if len(error_messages) > 0:
            utils.save_error_messages(error_file, error_messages)
            continue
        step5(
            work_dir, error_file, error_messages, META_DIR
        )
        if len(error_messages) > 0:
            utils.save_error_messages(error_file, error_messages)
            continue
        step6(work_dir, error_file, error_messages)
        if len(error_messages) > 0:
            utils.save_error_messages(error_file, error_messages)
            continue
        step7(work_dir, error_file, error_messages)
        if len(error_messages) > 0:
            utils.save_error_messages(error_file, error_messages)
            continue
        
    # Create error summary files
    utils.create_error_summary(DATA_DIR, ERROR_FILE_NAME)


def get_directories(DATA_DIR, include_dirs, exclude_dirs):
    all_dirs = glob.glob(os.path.join(DATA_DIR, "rad_*_*-*"))
    if include_dirs:
        return [f for f in all_dirs if os.path.basename(f) in include_dirs]
    elif exclude_dirs:
        return [f for f in all_dirs if os.path.basename(f) not in exclude_dirs]


def step1(preorigcopy_dir, work_dir):
    for input_file in glob.glob(os.path.join(preorigcopy_dir, "rad_*_*-*_*.csv")):
        basename = os.path.basename(input_file)
        output_file = os.path.join(
            work_dir, basename.replace("_preorigcopy.csv", ".csv")
        )

        # Proceed only if the input file is newer than the output file or it doesn't exist yet
        if utils.is_newer(input_file, output_file):
            # Copy preorigcopy file to work directory
            shutil.copyfile(input_file, output_file)


def step2(work_dir, error_file, error_messages):
    for input_file in glob.glob(os.path.join(work_dir, "rad_*_*-*_*_*.csv")):
        # Check if file is UTF-8 encoded
        error = utils.is_not_utf8_encoded(input_file, error_messages)

        if not error:
            # Copy the original file and remove any empty rows and columns
            error = utils.remove_empty_rows_cols(
                input_file, input_file, error_messages
            )
        
    return error_messages


def step3(work_dir, error_file, error_messages):
    for input_file in glob.glob(os.path.join(work_dir, "rad_*_*-*_*_DICT.csv")):       
        # Check DICT file for mandatory columns
        error = utils.check_dict(input_file, error_messages)

    return error_messages


def step4(work_dir, error_file, error_messages, HARMONIZED_DICT):
    for dict_file in glob.glob(os.path.join(work_dir, "rad_*_*-*_*_DICT.csv")):  
        # Match data fields to data elements in the dictionary files
        data_file = dict_file.replace("DICT", "DATA")
        error = utils.data_dict_matcher_new(
            data_file, dict_file, HARMONIZED_DICT, error_file, error_messages
        )
            
    return error_messages


def step5(work_dir, error_file, error_messages, META_DIR):
    for dict_file in glob.glob(os.path.join(work_dir,  "rad_*_*-*_*_DICT.csv")):
        any_error = False
        # Check for missing values in mandatory DICT fields
        error = utils.check_missing_values(dict_file, error_messages)
        if error:
            any_error = True

        # Check for valid field types in the DICT file
        error = utils.check_field_types(dict_file, error_messages)
        if error:
            any_error = True

        # Check provenance column for proper format
        error = utils.check_provenance(dict_file, error_messages)
        if error:
            any_error = True

        # Check if the data types in the DATA file match the data types specified in the DICT file
        data_file = dict_file.replace("DICT", "DATA")
        error = utils.check_data_type(data_file, dict_file, error_messages)
        if error:
            any_error = True

        # Check if the enumerated values used in the DATA file match the enumerations in the DICT file
        error = utils.check_enums(data_file, dict_file, error_messages)
        if error:
            any_error = True 

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

    return error_messages


def step6(work_dir, error_file, error_messages):
    for dict_file in glob.glob(os.path.join(work_dir,  "rad_*_*-*_*_DICT.csv")):
        utils.update_dict_file(dict_file, dict_file)

    return error_messages


def step7(work_dir, error_file, error_messages):
    for dict_file in glob.glob(os.path.join(work_dir,  "rad_*_*-*_*_DICT.csv")):
        dict_output_file = dict_file.replace("_DICT.csv", "_DICT_origcopy.csv")
        utils.convert_dict(dict_file, dict_output_file)


def main(include, exclude, reset):
    print("Phase2: Check and prepare origcopy files.")
    
    if include and exclude:
        print("Error: The '-include' and '-exclude' arguments cannot be specified at the same time.")
        sys.exit(1)
    elif include:
        print(f"including projects: {include}")
    elif exclude:
        print(f"excluding projects: {exclude}")
    else:
        print("Error: Use the '-include' or '-exclude' argument to specify which projects to process.")
        sys.exit(1)

    # Convert reset flag
    if reset:
        reset = True
        print("resetting project files with preorigcopy files")
    else:
        reset = False

    print()

    error_summary = os.path.join(DATA_DIR, "phase2_errors_summary.csv")
    error_all = os.path.join(DATA_DIR, "phase2_errors_details.csv")
    phase2_checker(DATA_DIR, include, exclude, META_DIR, HARMONIZED_DICT, reset)

    print()
    print(f"Error summary: {error_summary}")
    print(f"Error details: {error_all}")


if __name__ == '__main__':
    # Create the parser
    parser = argparse.ArgumentParser(description='Check and process preorigcopy files into orgicopy files.')

    # Add the arguments
    parser.add_argument("-include", type=str, required=False, help="Comma-separated list of projects to include.")
    parser.add_argument("-exclude", type=str, required=False, help="Comma-separated list of projects to exclude.")
    parser.add_argument('-reset', action='store_true', help="Reset project using files from preorigcopy")

    # Parse the arguments
    args = parser.parse_args()

    # Call the main function with the parsed arguments
    main(args.include, args.exclude, args.reset)