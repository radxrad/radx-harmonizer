#!/usr/bin/python3
import os
import sys
import glob
import pathlib
import shutil
import utils
import argparse

# required and optional fields in the RADx-rad dictionary files
# check order, unit later?
DICT_FIELDS = [
    "Variable / Field Name",
    "Field Label",
    "Section Header",
    "Field Type",
    "Unit",
    "Choices, Calculations, OR Slider Labels",
    "Field Note",
    "CDE Reference",
]


def phase2_checker(data_path, include_dirs, exclude_dirs, meta_data_template_path, clean_start=False):
    directories = get_directories(data_path, include_dirs, exclude_dirs)
    
    #directories = glob.glob(os.path.join(data_path, "rad_*_*-*"))

    for directory in directories:
        path = pathlib.PurePath(directory)
        preorigcopy_dir = os.path.join(directory, "preorigcopy")
        work_dir = os.path.join(directory, "work")

        # Skip and directories with Phase 1 errors
        phase1_error_file_name = "phase1_errors.csv"
        phase1_error_file = os.path.join(work_dir, phase1_error_file_name)
        if os.path.exists(phase1_error_file):
            print(f"skipping: {directory} due to Phase I errors")
            continue

        print(f"checking: {directory}")
        
        # Create work directory
        if clean_start:
            shutil.rmtree(work_dir, ignore_errors=True)
        os.makedirs(work_dir, exist_ok=True)

        # Clean up error file from a previous run
        error_file_name = "phase2_errors.csv"
        error_file = os.path.join(work_dir, error_file_name)

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
        step4(work_dir, error_file, error_messages)
        if len(error_messages) > 0:
            utils.save_error_messages(error_file, error_messages)
            continue
        step5(
            work_dir, error_file, error_messages, meta_data_template_path
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
    utils.create_error_summary(data_path, error_file_name)


def get_directories(data_path, include_dirs, exclude_dirs):
    all_dirs = glob.glob(os.path.join(data_path, "rad_*_*-*"))
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
        # If there is an error, try to convert to an iso-encoded file
        if error:
            # Check if file is ISO encoded
            error = utils.is_not_iso_encoded(input_file, error_messages)
            # If the file can be read ISO encoded, try to convert to UTF-8
        else:
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


def step4(work_dir, error_file, error_messages):
    for dict_file in glob.glob(os.path.join(work_dir, "rad_*_*-*_*_DICT.csv")):  
        # Match data fields to data elements in the dictionary files
        data_file = dict_file.replace("DICT", "DATA")
        error = utils.data_dict_matcher_new(
            data_file, dict_file, error_file, error_messages
        )
            
    return error_messages


def step5(work_dir, error_file, error_messages, meta_data_template_path):
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
            meta_output_file = dict_file.replace("DICT.csv", "META_converted.csv")
            error = utils.update_meta_data(
                meta_file,
                meta_output_file,
                meta_data_template_path,
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
        utils.convert_dict(dict_file, dict_file)


def main(include, exclude, reset):
    # Convert reset flag
    if reset:
        reset = True
        print("Resetting project")
    else:
        reset = False

    if include and exclude:
        print("Error: The '-include' and '-exclude' arguments cannot be specified at the same time.")
        sys.exit(1)
    elif include:
        print(f"Included projects: {include}")
    elif exclude:
        print(f"Excluded projects: {exclude}")
    else:
        print("Error: Use the '-include' or '-exclude' argument to specify which projects to process.")
        sys.exit(1)

    # Set path to data and metadata directories
    # directory = "r:\data_harmonized"
    # meta = "r:\meta"]
    directory = "../data_harmonized"
    meta = "../meta"
    
    error_summary = os.path.join(directory, "phase2_errors.csv")
    error_all = os.path.join(directory, "phase2_errors_all.csv")
    phase2_checker(directory, include, exclude, meta, reset)
    
    print(
        f"Phase 2: Check error summary: {error_summary} and {error_all} for errors in the preorigcopy files."
    )


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