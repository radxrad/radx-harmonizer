#!/usr/bin/python3
import os
import glob
import pathlib
import pandas as pd
import csv

required_fields = {
    "Variable / Field Name",
    "Field Label",
    "Section Header",
    "Field Type",
    "Unit",
    "Choices, Calculations, OR Slider Labels",
    "Field Note",
    "CDE Reference",
}


def file_is_missing(directory, error_messages):
    all_files = set(glob.glob(os.path.join(directory, "*")))
    data_files = set(
        glob.glob(os.path.join(directory, "rad_*_*-*_DATA_preorigcopy.csv"))
    )
    dict_files = set(
        glob.glob(os.path.join(directory, "rad_*_*-*_DICT_preorigcopy.csv"))
    )
    meta_files = set(
        glob.glob(os.path.join(directory, "rad_*_*-*_META_preorigcopy.csv"))
    )

    # TODO: check if directory and file names rad_XXXX_YYYY-ZZZZ match!

    error = False
    # Check for files that don't match the file naming convention
    extra_files = all_files - data_files - dict_files - meta_files
    for extra_file in extra_files:
        message = "Unrecognized file name"
        error_messages.append(
            {
                "severity": "ERROR",
                "filename": os.path.basename(extra_file),
                "message": message,
            }
        )
        error = True

    # Check that the number of DATA, DICT, and META files is the same
    if len(data_files) != len(dict_files) or len(data_files) != len(meta_files):
        message = "DATA, DICT, META file mismatch"
        error_messages.append(
            {"severity": "ERROR", "filename": directory, "message": message}
        )
        error = True

    for data_file in data_files:
        # Check for missing DICT files
        dict_file = data_file.replace("_DATA_preorigcopy.csv", "_DICT_preorigcopy.csv")
        if not dict_file in dict_files:
            message = "DICT file missing"
            error_messages.append(
                {
                    "severity": "ERROR",
                    "filename": os.path.basename(dict_file),
                    "message": message,
                }
            )
            error = True

        # Check for missing META files
        meta_file = data_file.replace("_DATA_preorigcopy.csv", "_META_preorigcopy.csv")
        if not meta_file in meta_files:
            message = "META file missing"
            error_messages.append(
                {
                    "severity": "ERROR",
                    "filename": os.path.basename(meta_file),
                    "message": message,
                }
            )
            error = True

    return error, error_messages


def check_meta_file(filename, error_messages):
    data = pd.read_csv(
        filename,
        encoding="utf8",
        dtype=str,
        keep_default_na=False,
        skip_blank_lines=False,
    )
    columns = data.columns

    error = False

    if len(columns) != 3:
        message = f"Metadata file has {len(columns)} columns, 3 columns are required"
        error_messages.append(
            {
                "severity": "ERROR",
                "filename": os.path.basename(filename),
                "message": message,
            }
        )
        error = True

    if error:
        return error, error_messages

    # check column names
    if columns[0] != "Field Label":
        message = "Field Label column missing"
        error_messages.append(
            {
                "severity": "ERROR",
                "filename": os.path.basename(filename),
                "message": message,
            }
        )
        error = True
    if columns[1] != "Choices":
        message = "Choices column missing"
        error_messages.append(
            {
                "severity": "ERROR",
                "filename": os.path.basename(filename),
                "message": message,
            }
        )
        error = True
    if columns[2] != "Description":
        message = "Description column missing"
        error_messages.append(
            {
                "severity": "ERROR",
                "filename": os.path.basename(filename),
                "message": message,
            }
        )
        error = True

    if error:
        return error, error_messages

    # check the number of data files
    filenames = data[data["Field Label"] == "number_of_datafiles_in_this_package"]
    if filenames.shape[0] != 1:
        message = "Row 'number_of_datafiles_in_this_package' is missing"
        error_messages.append(
            {
                "severity": "ERROR",
                "filename": os.path.basename(filename),
                "message": message,
            }
        )
        error = True

    if error:
        return error, error_messages

    num_files = filenames["Choices"].tolist()
    if num_files[0] != "1":
        message = f"number_of_datafiles_in_this_package is {num_files[0]}, it must be 1"
        error_messages.append(
            {
                "severity": "ERROR",
                "filename": os.path.basename(filename),
                "message": message,
            }
        )
        error = True

    # check data file name
    filenames = data[
        data["Field Label"] == "datafile_names - add_additional_rows_as_needed"
    ]
    if filenames.shape[0] != 1:
        message = "Row 'datafile_names - add_additional_rows_as_needed' is missing"
        error_messages.append(
            {
                "severity": "ERROR",
                "filename": os.path.basename(filename),
                "message": message,
            }
        )
        error = True

    if error:
        return error, error_messages

    data_file = os.path.basename(filename).replace("_META_", "_DATA_")
    data_files = filenames["Choices"].tolist()
    if data_files[0] != data_file:
        message = f"Data file name: {data_files[0]} doesn't match"
        error_messages.append(
            {
                "severity": "ERROR",
                "filename": os.path.basename(filename),
                "message": message,
            }
        )
        error = True

    description = filenames["Description"].tolist()
    description = description[0]
    if description == "":
        message = "Data file description is missing"
        error_messages.append(
            {
                "severity": "WARN",
                "filename": os.path.basename(filename),
                "message": message,
            }
        )
        error = True

    return error, error_messages


def is_not_utf8_encoded(filename, error_messages):
    import traceback

    error = False
    try:
        data = pd.read_csv(filename, encoding="utf8", skip_blank_lines=False)
    except Exception:
        message = f"Not utf-8 encoded or invalid csv file: {traceback.format_exc().splitlines()[-1]}"
        error_messages.append(
            {
                "severity": "ERROR",
                "filename": os.path.basename(filename),
                "message": message,
            }
        )
        error = True

    return error, error_messages


def is_not_iso_encoded(filename, error_messages):
    import traceback

    error = False
    try:
        data = pd.read_csv(filename, encoding="ISO-8859-1", skip_blank_lines=False)
    except Exception:
        # this traceback has an empty last line.
        # use the second from last line instead.
        message = f"Not ISO-8859-1 encoded or invalid csv file: {traceback.format_exc().splitlines()[-2]}"
        error_messages.append(
            {
                "severity": "ERROR",
                "filename": os.path.basename(filename),
                "message": message,
            }
        )
        error = True

    return error, error_messages


def convert_iso_to_utf8(orig_filename, fixed_filename, error_messages):
    try:
        data = pd.read_csv(
            orig_filename,
            encoding="ISO-8859-1",
            dtype=str,
            keep_default_na=False,
            skip_blank_lines=False,
        )
        data.to_csv(fixed_filename, encoding="utf-8", index=False)
        message = "File was automatically converted to utf-8"
        error_messages.append(
            {
                "severity": "WARN",
                "filename": os.path.basename(fixed_file),
                "message": message,
            }
        )
    except Exception:
        message = traceback.format_exc().splitlines()[-1]
        error_messages.append(
            {
                "severity": "ERROR",
                "filename": os.path.basename(filename),
                "message": message,
            }
        )
        error = True
        return error, error_messages

    return is_not_utf8_encoded(fixed_filename, error_messages)


def check_column_names(data, error_messages):
    error = False
    if len(data.columns) != data.shape[1]:
        message = "Number of columns in header do not match the data"
        error_messages.append(
            {
                "severity": "ERROR",
                "filename": os.path.basename(filename),
                "message": message,
            }
        )
        error = True
    for col in data.columns:
        col_stripped = col.strip()
        if col_stripped != col:
            message = f"column header: '{col}' contains spaces"
            error_messages.append(
                {
                    "severity": "ERROR",
                    "filename": os.path.basename(filename),
                    "message": message,
                }
            )
            error = True
        if col_stripped == "":
            message = "Empty column name"
            error_messages.append(
                {
                    "severity": "ERROR",
                    "filename": os.path.basename(filename),
                    "message": message,
                }
            )
            error = True
        if "Unnamed" in col:
            message = f"Unnamed column: {col}"
            error_messages.append(
                {
                    "severity": "ERROR",
                    "filename": os.path.basename(filename),
                    "message": message,
                }
            )
            error = True

    return error, error_messages


def remove_empty_rows_cols(input_file, output_file, error_messages):
    data = pd.read_csv(
        input_file,
        encoding="utf8",
        dtype=str,
        keep_default_na=False,
        skip_blank_lines=False,
    )
    # TODO remove whitespace from the header

    # remove leading and trailing whitespace
    data = data.map(lambda x: x.strip())
    # identify rows with all empty strings
    empty_row_mask = data.eq("").all(axis=1)
    data = data[~empty_row_mask]
    # identify columns with all empty strings
    empty_col_mask = data.eq("").all(axis=0)
    # select columns where the mask is False
    columns_to_keep = data.columns[~empty_col_mask]
    # filter the DataFrame by selecting the desired columns
    data = data[columns_to_keep]

    data.dropna(axis="rows", how="all", inplace=True)
    data.dropna(axis="columns", how="all", inplace=True)

    error, error_messages = check_column_names(data, error_messages)
    if error:
        return error, error_messsages

    data.to_csv(output_file, index=False)
    return False, error_messages


def has_empty_rows(filename, error_messages):
    data = pd.read_csv(filename, dtype=str, encoding="utf8", skip_blank_lines=False)
    filtered_data = data.dropna(axis="rows", how="all")

    error = False
    if filtered_data.shape[0] != data.shape[0]:
        message = "file has empty rows"
        error_messages.append(
            {
                "severity": "ERROR",
                "filename": os.path.basename(filename),
                "message": message,
            }
        )

    return error, error_messages


def is_newer(filename1, filename2):
    # the second file doesn't exist yet
    if not os.path.isfile(filename2):
        return True
    # check if the first file is newer than the second file
    return os.path.getmtime(filename1) > os.path.getmtime(filename2)


def get_input_output_files_for_next_step(input_file):
    # _tofix file are not suitable as input files
    if "_tofix" in input_file:
        return None, None

    if os.path.isfile(input_file):
        has_fixed_file = "_fixed.csv" in input_file
        if has_fixed_file:
            parts = input_file.replace("_fixed", "").rsplit("_", maxsplit=1)
        else:
            parts = input_file.rsplit("_", maxsplit=1)

        prefix = parts[0]
        postfix = int(parts[1].replace(".csv", ""))
        postfix += 1
        output_file = f"{prefix}_{postfix}.csv"

        return input_file, output_file

    return None, None


def create_error_summary(directories, work_path, error_file_name):
    error_dict = []
    for directory in directories:
        path = pathlib.PurePath(directory)
        work_dir = os.path.join(work_path, path.name)
        error_file = os.path.join(work_dir, error_file_name)

        if os.path.exists(error_file):
            errors = pd.read_csv(error_file)
            num_errors = errors.shape[0]
            error_dict.append({"error_file": error_file, "errors": num_errors})

    error_df = pd.DataFrame(error_dict)
    error_df.to_csv(os.path.join(work_path, error_file_name), index=False)


def save_error_file(error_messages, error_file):
    df = pd.DataFrame(error_messages)
    if len(df) > 0:
        df.to_csv(os.path.join(error_file), index=False)

