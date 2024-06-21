#!/usr/bin/python3
import os
import glob
import pathlib
import shutil
import utils

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


def phase2_checker_new(data_path, meta_data_template_path, clean_start=False):
    directories = glob.glob(os.path.join(data_path, "rad_*_*-*"))

    for directory in directories:
        path = pathlib.PurePath(directory)
        preorigcopy_dir = os.path.join(directory, "preorigcopy")
        work_dir = os.path.join(directory, "work")

        if clean_start:
            shutil.rmtree(work_dir, ignore_errors=True)

        os.makedirs(work_dir, exist_ok=True)

        error_file_name = "phase2_errors.csv"
        error_file = os.path.join(work_dir, error_file_name)
        # clean up error file from a previous run
        # TODO How to remove errors from a previous run?
        if clean_start:
            if os.path.exists(error_file):
                os.remove(error_file)

        step1(preorigcopy_dir, work_dir)

        error_messages = []
        error_messages = step2(work_dir, error_file, error_messages)
        error_messages = step3(work_dir, error_file, error_messages)
        error_messages = step4(work_dir, error_file, error_messages)
        error_messages = step5(
            work_dir, error_file, error_messages, meta_data_template_path
        )


def step1(preorigcopy_dir, work_dir):
    for input_file in glob.glob(os.path.join(preorigcopy_dir, "rad_*_*-*_*.csv")):
        basename = os.path.basename(input_file)
        output_file = os.path.join(
            work_dir, basename.replace("_preorigcopy.csv", "_1.csv")
        )

        # Proceed only if the input file is newer than the output file or it doesn't exist yet
        if not utils.is_newer(input_file, output_file):
            continue

        # Copy preorigcopy file to work directory
        shutil.copyfile(input_file, output_file)

        # Remove any working copies from a previous version
        tofix_file = output_file.replace("_1.csv", "_1_tofix.csv")
        if os.path.exists(tofix_file):
            os.remove(tofix_file)
        fixed_file = output_file.replace("_1.csv", "_1_fixed.csv")
        if os.path.exists(fixed_file):
            os.remove(fixed_file)


def step2(work_dir, error_file, error_messages):
    for input_file in glob.glob(os.path.join(work_dir, "rad_*_*-*_*_1.csv")):
        if not (input_file := utils.get_input_file(input_file)):
            continue

        # Proceed only if the input file is newer than the output file
        output_file = utils.get_output_file(input_file)
        if not utils.is_newer(input_file, output_file):
            continue

        # Check if file is UTF-8 encoded
        error, error_messages = utils.is_not_utf8_encoded(input_file, error_messages)
        # If there is an error, try to convert iso-encoded file to utf8-encoded files
        if error:
            # Check if file is ISO encoded
            error, error_messages = utils.is_not_iso_encoded(input_file, error_messages)
            # If the file can be read ISO encoded, try to convert to UTF-8
            if not error:
                fixed_file = utils.get_fixed_file(input_file)
                error, error_messages = utils.convert_iso_to_utf8(
                    input_file, fixed_file, error_messages
                )
        else:
            # Copy the original file which is already utf-8 encoded
            error, error_messages = utils.remove_empty_rows_cols(
                input_file, output_file, error_messages
            )

        if error:
            # Create a "_tofix" file for manual fixing
            error_messages = utils.save_tofix_version(input_file, error_file, error_messages)

    return error_messages


def step3(work_dir, error_file, error_messages):
    for input_file in glob.glob(os.path.join(work_dir, "rad_*_*-*_*_2.csv")):
        if not (input_file := utils.get_input_file(input_file)):
            continue

        # Proceed only if the input file is newer than the output file
        output_file = utils.get_output_file(input_file)
        if not utils.is_newer(input_file, output_file):
            continue

        if "_DICT_" in input_file:
            # Check DICT file for mandatory columns
            error, error_messages = utils.check_dict(input_file, error_messages)
            if error:
                # Create a "_tofix" file for manual fixing
                error_messages = utils.save_tofix_version(input_file, error_file, error_messages)
            else:
                error_messages = utils.save_next_version(input_file, output_file, error_file, error_messages)
        else:
            # DATA and META files are passed through to the next step
            error_messages = utils.save_next_version(input_file, output_file, error_file, error_messages)

    return error_messages


def step4(work_dir, error_file, error_messages):
    for dict_file in glob.glob(os.path.join(work_dir, "rad_*_*-*_DICT_3.csv")):
        # Copy META file to the next version
        meta_file = dict_file.replace("DICT", "META")
        meta_output_file = utils.get_output_file(meta_file)
        if utils.is_newer(meta_file, meta_output_file):
            shutil.copyfile(meta_file, meta_output_file)

        data_file = dict_file.replace("DICT", "DATA")
        #print("step4: data_file:", data_file, utils.get_input_file(data_file))
        if not (data_file := utils.get_input_file(data_file)):
            continue
        if not (dict_file := utils.get_input_file(dict_file)):
            continue

        #print("step3:", data_file, dict_file)
        # Proceed only if the input files are newer than the output file
        dict_output_file = utils.get_output_file(dict_file)
        data_output_file = utils.get_output_file(data_file)
        if (not utils.is_newer(dict_file, dict_output_file)) and (not utils.is_newer(data_file, data_output_file)):
            continue

        # # Copy DATA file to the next version
        # error_messages = utils.save_next_version_without_none(data_file, data_output_file, error_file, error_messages)
        # utils.save_error_file(error_messages, error_file)

        # Match data fields to data elements in the dictionary files
        error, error_messages = utils.data_dict_matcher(
            data_file, dict_file, error_file, error_messages
        )
        if error:
            print("step4: tofix:", dict_file)
            error_messages = utils.save_tofix_version(dict_file, error_file, error_messages)

        # Copy DATA file to the next version
        error_messages = utils.save_next_version_without_none(data_file, data_output_file, error_file, error_messages)
        utils.save_error_file(error_messages, error_file)

            
    return error_messages


def step5(work_dir, error_file, error_messages, meta_data_template_path):
    for dict_file in glob.glob(os.path.join(work_dir, "rad_*_*-*_DICT_4.csv")):
        # Copy META file to the next version
        meta_file = dict_file.replace("DICT", "META")
        meta_output_file = utils.get_output_file(meta_file)

        data_file = dict_file.replace("DICT", "DATA")
        if not (data_file := utils.get_input_file(data_file)):
            continue

        if not (dict_file := utils.get_input_file(dict_file)):
            continue

        # Proceed only if the input files are newer than the output file
        dict_output_file = utils.get_output_file(dict_file)
        if dict_output_file and not utils.is_newer(dict_file, dict_output_file):
            continue
        data_output_file = utils.get_output_file(data_file)
        if data_output_file and not utils.is_newer(data_file, data_output_file):
            continue

        # 
        any_error = False
        # Check for missing values in mandatory DICT fields
        error, error_messages = utils.check_missing_values(dict_file, error_messages)
        if error:
            error_messages = utils.save_tofix_version(data_file, error_file, error_messages)
            any_error = True

        # Check for valid field types in the DICT file
        error, error_messages = utils.check_field_types(dict_file, error_messages)
        if error:
            error_messages = utils.save_tofix_version(data_file, error_file, error_messages)
            any_error = True

        # Check if the data types in the DATA file match the data types specified in the DICT file
        error, error_messages = utils.check_data_type(data_file, dict_file, error_messages)
        if error:
            # The error could either be in the DATA or DICT file
            error_messages = utils.save_tofix_version(data_file, error_file, error_messages)
            error_messages = utils.save_tofix_version(dict_file, error_file, error_messages)
            print("step5: data type errors")
            any_error = True

        # Check if the enumerated values used in the DATA file match the enumerations in the DICT file
        error, error_messages = utils.check_enums(data_file, dict_file, error_messages)
        if error:
            error_messages = utils.save_tofix_version(data_file, error_file, error_messages)
            print("step5: enum errors")
            any_error = True 

        
        if not any_error:
            # Use the metadata templates and combine them with data from the DATA file to create an updated META file
            error, error_messages = utils.update_meta_data(
                meta_file,
                meta_output_file,
                meta_data_template_path,
                data_file,
                error_messages,
            )
            if error:
                error_messages = utils.save_tofix_version(meta_file, error_file, error_messages)
            else:
                error_messages = utils.save_next_version(meta_file, meta_output_file, error_file, error_messages)

            error_messages = utils.save_next_version(dict_file, dict_output_file, error_file, error_messages)
            error_messages = utils.save_next_version(data_file, data_output_file, error_file, error_messages)

    return error_messages


if __name__ == "__main__":
    phase2_checker_new("../data_harmonized", "../meta", False)
    print(
        "Phase 2: Check file: ../data_harmonized/phase2_errors.csv for a summary of errors."
    )
