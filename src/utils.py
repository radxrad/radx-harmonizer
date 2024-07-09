#!/usr/bin/python3
import os
import glob
import pathlib
import shutil
import traceback
import re
import hashlib
import numpy as np
import pandas as pd


# Columns required for dictionary files
MANDATORY_COLUMNS = {
    "Variable / Field Name", 
    "Field Label", 
    "Field Type",
}

# All columns that are allowed in a dictionary files
ALL_COLUMNS = {
    "Variable / Field Name",
    "Section Header",
    "Field Type",
    "Field Label",
    "Choices, Calculations, OR Slider Labels",
    "Field Note",
    "Text Validation Type OR Show Slider Number",
    "Text Validation Min",
    "Text Validation Max",
    "Branching Logic (Show field only if...)",
    "Unit",
    "CDE Reference",
}

# Allowed field types in dictionary files
ALLOWED_TYPES = {
    "text",
    "integer",
    "float",
    "date",
    "time",
    "timezone",
    "zipcode",
    "url",
    "sequence",
    "list",
    "category",
    "yesno",
    "radio",
    "dropdown",
    "checkbox",
}

# None values to be replaced by empty string
NULL_VALUES = ["N/A", "NA", "NULL", "NaN", "None", "n/a", "nan", "null"]


enum_pattern_int = r"(\d+),\s*([^|]+)\s*(?:\||$)"  # Example: 1, Male | 2, Female | 3, Intersex | 4, None of these describe me
enum_pattern_str = r"([A-Z]+),\s*([^|]+)\s*(?:\||$)"  # Example: AL, Alabama | AK, Alaska | AS, American Samoa

column_map = {"Variable / Field Name": "Id", "Field Label": "Label", "Section Header": "Section", "Field Type": "Datatype", "Unit": "Unit", "Units": "Unit", "Choices, Calculations, OR Slider Labels": "Enumeration", "Field Note": "Notes", "CDE Reference": "Provenance"}

# Field names that contain specimen information
SPECIMEN_COLUMNS = ["specimen_type", "virus_sample_type", "sample_media", "sample_type"]

def append_error(message, filename, error_messages):
    error_messages.append(
        {
            "severity": "ERROR",
            "filename": os.path.basename(filename),
            "message": message,
        }
    )
    return True
    #return error_messages


def append_warning(message, filename, error_messages):
    error_messages.append(
        {
            "severity": "WARN",
            "filename": os.path.basename(filename),
            "message": message,
        }
    )
    return True


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

    # TODO: check if directory and file names rad_XXXX_YYYY-ZZZZ match! _> can to this already in Phase1!

    error = False
    # Check for files that don't match the file naming convention
    extra_files = all_files - data_files - dict_files - meta_files
    for extra_file in extra_files:
        message = "Unrecognized file name"
        error = append_error(message, extra_file, error_messages)
        #error = True

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
            error = append_error(message, dict_file, error_messages)
            #error = True

        # Check for missing META files
        meta_file = data_file.replace("_DATA_preorigcopy.csv", "_META_preorigcopy.csv")
        if not meta_file in meta_files:
            message = "META file missing"
            error = append_error(message, meta_file, error_messages)
            #error = True

    return error


def check_meta_file(filename, error_messages):
    error = False
    try:
        data = pd.read_csv(
            filename,
            encoding="utf8",
            dtype=str,
            keep_default_na=False,
            skip_blank_lines=False,
        )
    except Exception:
        message = f"Invalid csv file: {traceback.format_exc().splitlines()[-2]}"
        error = append_error(message, filename, error_messages)
        #error = True
        return error

    data.rename(columns={"Description": "Descriptions"}, inplace=True)
    
    columns = data.columns

    if len(columns) != 3:
        message = f"Metadata file has {len(columns)} columns, 3 columns are required"
        error = append_error(message, filename, error_messages)
        #error = True

    if error:
        return error

    required_columns = ["Field Label", "Choices", "Descriptions"]

    # Check column names
    for i, expected_column in enumerate(required_columns):
        if columns[i] != expected_column:
            message = f"{expected_column} column missing"
            error = append_error(message, filename, error_messages)
            #error = True
    # # check column names
    # if columns[0] != "Field Label":
    #     message = "Field Label column missing"
    #     error = append_error(message, filename, error_messages)
    #     #error = True
    # if columns[1] != "Choices":
    #     message = "Choices column missing"
    #     error = append_error(message, filename, error_messages)
    #     #error = True
    # if columns[2] != "Descriptions":
    #     message = "Description column missing"
    #     error = append_error(message, filename, error_messages)
    #     #error = True

    if error:
        return error

    # check the number of data files
    filenames = data[data["Field Label"] == "number_of_datafiles_in_this_package"]
    if filenames.shape[0] != 1:
        message = "Row 'number_of_datafiles_in_this_package' is missing"
        error = append_error(message, filename, error_messages)
        #error = True

    if error:
        return error

    num_files = filenames["Choices"].tolist()
    if num_files[0] != "1":
        message = f"number_of_datafiles_in_this_package is {num_files[0]}, it must be 1"
        error = append_error(message, filename, error_messages)
        #error = True

    # check data file name
    filenames = data[
        data["Field Label"] == "datafile_names - add_additional_rows_as_needed"
    ]
    if filenames.shape[0] != 1:
        message = "Row 'datafile_names - add_additional_rows_as_needed' is missing"
        error = append_error(message, filename, error_messages)
        #error = True

    if error:
        return error

    data_file = os.path.basename(filename).replace("_META_", "_DATA_")
    data_files = filenames["Choices"].tolist()
    if data_files[0] != data_file:
        message = f"Data file name: {data_files[0]} doesn't match"
        error = append_error(message, filename, error_messages)
        #error = True

    description = filenames["Descriptions"].tolist()
    description = description[0]
    if description == "":
        message = "Data file description is missing"
        error = append_error(message, filename, error_messages)
        #error = True

    return error


def is_not_utf8_encoded(filename, error_messages):
    error = False
    try:
        data = pd.read_csv(filename, encoding="utf8", low_memory=False, skip_blank_lines=False)
    except Exception:
        message = f"Not utf-8 encoded or invalid csv file: {traceback.format_exc().splitlines()[-1]}"
        error = append_error(message, filename, error_messages)

    return error


def is_not_iso_encoded(filename, error_messages):
    error = False
    try:
        data = pd.read_csv(filename, encoding="ISO-8859-1", skip_blank_lines=False)
    except Exception:
        # this traceback has an empty last line.
        # use the second from last line instead.
        message = f"Not ISO-8859-1 encoded or invalid csv file: {traceback.format_exc().splitlines()[-2]}"
        error = append_error(message, filename, error_messages)
        #error = True

    return error


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
        error = append_warning(message, fixed_filename, error_messages)
    except Exception:
        message = f"Not ISO-8859-1 encoded or invalid csv file: {traceback.format_exc().splitlines()[-1]}"
        error = append_error(message, filename, error_messages)
        return error

    return is_not_utf8_encoded(fixed_filename, error_messages)


def check_column_names(data, filename, error_messages):
    error = False
    if len(data.columns) != data.shape[1]:
        message = "Number of columns in header do not match the data"
        error = append_error(message, filename, error_messages)
        #error = True
    for col in data.columns:
        col_stripped = col.strip()
        if col_stripped != col:
            message = f"column header: '{col}' contains spaces"
            error = append_error(message, filename, error_messages)
            #error = True
        if col_stripped == "":
            message = "Empty column name"
            error = append_error(message, filename, error_messages)
            #error = True
        if "Unnamed" in col:
            message = f"Unnamed column: {col}"
            error = append_error(message, filename, error_messages)
            #error = True

    return error


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
    # in Pandas < 2.1.0
    # data = data.applymap(lambda x: x.strip())
    
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

    error = check_column_names(data, input_file, error_messages)
    if error:
        return error

    data.to_csv(output_file, index=False)
    return False


def has_empty_rows(filename, error_messages):
    data = pd.read_csv(filename, dtype=str, encoding="utf8", skip_blank_lines=False)
    filtered_data = data.dropna(axis="rows", how="all")

    error = False
    if filtered_data.shape[0] != data.shape[0]:
        message = "file has empty rows"
        error = append_error(message, filename, error_messages)

    return error


def is_newer(filename1, filename2):
    # the second file doesn't exist yet
    if not os.path.isfile(filename2):
        return True
    # check if the first file is newer than the second file
    return os.path.getmtime(filename1) > os.path.getmtime(filename2)


def get_input_file(input_file):
    # If a fixed version of a file exists, return it instead of the original version
    fixed_file = input_file.replace(".csv", "_fixed.csv")
    if os.path.isfile(fixed_file) and os.path.isfile(input_file):
        return fixed_file
    # If there is a version to be fixed, don't process input file
    tofix_file = input_file.replace(".csv", "_tofix.csv")
    if os.path.isfile(tofix_file) and os.path.isfile(input_file):
        return None
    # Return the original file for further processing
    if os.path.isfile(input_file):
        return input_file
    else:
        return None


def get_output_file(input_file):
    # Remove _fixed postfix if present
    input_file = input_file.replace("_fixed.csv", ".csv")
    return increment_file_version(input_file)


def get_tofix_file(input_file):
    # Remove _fixed postfix if present
    input_file = input_file.replace("_fixed.csv", ".csv")
    tofix_file = input_file.replace(".csv", "_tofix.csv")
    return tofix_file


def get_fixed_file(input_file):
    # Remove _fixed postfix if present
    input_file = input_file.replace("_fixed.csv", ".csv")
    fixed_file = input_file.replace(".csv", "_fixed.csv")
    return fixed_file


def increment_file_version(filename):
    if "_fixed.csv" in filename:
        parts = filename.replace("_fixed", "").rsplit("_", maxsplit=1)
    else:
        parts = filename.rsplit("_", maxsplit=1)

    prefix = parts[0]
    postfix = int(parts[1].replace(".csv", ""))
    postfix += 1
    return f"{prefix}_{postfix}.csv"


def create_error_summary(data_path, error_filename):
    error_dict = []
    error_all = []

    directories = glob.glob(os.path.join(data_path, "rad_*_*-*"))
    for directory in directories:
        path = pathlib.PurePath(directory)
        work_dir = os.path.join(directory, "work")
        error_file = os.path.join(work_dir, error_filename)

        if os.path.exists(error_file):
            errors = pd.read_csv(error_file)
            num_errors = errors.shape[0]
            error_dict.append({"error_file": error_file, "errors": num_errors})
            error_all.append(errors)

    # Create error file summary
    error_df = pd.DataFrame(error_dict)
    error_df.to_csv(os.path.join(data_path, error_filename), index=False)
    
    # Create comprehensive data file with all error messages
    error_df_all = pd.concat(error_all)
    all_error_filename = error_filename.replace(".csv", "_all.csv")
    error_df_all.to_csv(os.path.join(data_path, all_error_filename), index=False)
    


def save_error_file(error_messages, error_file):
    df = pd.DataFrame(error_messages)
    if len(df) > 0:
        df.to_csv(error_file, index=False)


def update_error_file(error_file, filename, error_messages):
    # Extract the basename without path and suffix
    basename = filename.replace("_fixed.csv", ".csv")
    basename = os.path.basename(basename)

    # Remove error messages for issues that have been fixed
    for message in error_messages:
        if message["filename"] == basename:
            error_messages.remove(message)

    if not os.path.exists(error_file):
        return error_messages

    errors = pd.read_csv(error_file)

    # Remove error messages from the error file
    errors = errors[errors["filename"] != basename]
    if errors.shape[0] == 0:
        os.remove(error_file)
    else:
        errors.to_csv(error_file, index=False)

    return error_messages
        

def get_empty_columns(df):
    empty_columns = [col for col in df.columns if df[col].eq("").all()]
    return empty_columns


def save_next_version(input_file, output_file, error_file, error_messages):
    shutil.copyfile(input_file, output_file)
    error_messages = update_error_file(error_file, input_file, error_messages)
    return error_messages


def save_tofix_version(input_file, error_file, error_messages):
    tofix_file = get_tofix_file(input_file)
    shutil.copyfile(input_file, tofix_file)
    save_error_file(error_messages, error_file)


def get_num_empty_rows(df, field_name):
    df_empty = df[df[field_name] == ""].copy()
    return int(df_empty.shape[0])


def check_dict(filename, error_messages):
    df = pd.read_csv(filename, dtype=str, keep_default_na=False, skip_blank_lines=False)

    # Find missing mandatory columns
    columns = set(df.columns)
    missing_columns = MANDATORY_COLUMNS - columns

    error = False
    if len(missing_columns) > 0:
        message = f"Missing columns: {missing_columns}"
        error = append_error(message, filename, error_messages)
        #error = True

    # Find unexpected columns
    unexpected_columns = columns - ALL_COLUMNS
    if len(unexpected_columns) > 0:
        message = f"Unexpected columns: {unexpected_columns}, can be ignored, unless they are typos"
        append_warning(message, filename, error_messages)
        #error = True

    return error


def check_missing_values(filename, error_messages):
    df = pd.read_csv(filename, dtype=str, keep_default_na=False, skip_blank_lines=False)
    error = False

    # check for missing values in the required columns
    for field_name in MANDATORY_COLUMNS:
        num_empty_rows = get_num_empty_rows(df, field_name)
        if num_empty_rows > 0:
            message = f"Column: `{field_name}` has {num_empty_rows} empty values out of {df.shape[0]} rows"
            error = append_error(message, filename, error_messages)
            #error = True

    return error


def check_field_types(filename, error_messages):
    df = pd.read_csv(filename, dtype=str, keep_default_na=False, skip_blank_lines=False)
    field_types = set(df["Field Type"].unique())
    invalid_field_types = field_types - ALLOWED_TYPES
    error = False
    if len(invalid_field_types) > 0:
        message = f"Invalid Field Types: {list(invalid_field_types)}"
        error = append_error(message, filename, error_messages)
        #error = True

    return error


def save_next_version(input_file, output_file, error_file, error_messages):
    shutil.copyfile(input_file, output_file)
    error_messages = update_error_file(error_file, input_file, error_messages)


def save_next_version_without_none(input_file, output_file, error_file, error_messages):
    error_messages = update_error_file(error_file, input_file, error_messages)
    data = pd.read_csv(input_file, dtype=str, skip_blank_lines=False)
    # Add warning messages for columns with "null" values
    for column in list(data.columns):
        types = get_column_type(data, column)
        for col_type in types:
            if col_type in NULL_VALUES:
                message = f"Removed null value: {col_type} in column: {column}"
                error = append_warning(message, input_file, error_messages)
                
    data.fillna("",inplace=True)
    data.to_csv(output_file, index=False)

    
def check_data_type(data_file, dict_file, error_messages):
    data = pd.read_csv(
        data_file, dtype=str, keep_default_na=False, skip_blank_lines=False
    )
    dict_types = get_dictionary_data_types(dict_file)

    error = False
    for column in list(data.columns):
        # Ignore the "type" column. It is used store temporay data types.
        if column == "type":
            continue
        types = get_column_type(data, column)
        dict_type = dict_types.get(column)
        if len(types) == 1 and not types[0] == dict_type:
            # Some identifier columns have integer values but are declared as strings
            if dict_type == "string" and types[0] == "integer":
                continue
            # Integer values are ok in float columns
            if dict_type == "float" and types[0] == "integer":
                continue
            message = f"Invalid data type in column: {column}: {types[0]} in DATA vs. {dict_type} in DICT"
            #print("check_data_type:", message, data_file, dict_file)
            error = append_error(message, data_file, error_messages)
            message = f"Invalid data type in column: {column}: {dict_type} in DICT vs. {types[0]} in DATA"
            error = append_error(message, dict_file, error_messages)
            #error = True
        elif len(types) > 1:
            # mixed types are ok if the type in the dictionary is defined as string
            if dict_type == "string":
                continue
            else:
                message = f"Mixed data types in column: {column}: {types} in DATA vs. {dict_type} IN DICT"
                error = append_error(message, data_file, error_messages)
                error = append_error(message, dict_file, error_messages)
                #error = True

    return error


def get_dictionary_data_types(dict_file):
    dictionary = pd.read_csv(
        dict_file, dtype=str, keep_default_na=False, skip_blank_lines=False
    )
    dictionary["type"] = dictionary.apply(convert_data_type, axis=1)
    dict_types = dictionary.set_index("Variable / Field Name")["type"].to_dict()
    return dict_types


def get_column_type(df, fieldname):
    df["type"] = df[fieldname].apply(determine_type)
    types = list(df["type"].unique())

    # Ignore blank values, they are ok
    if "blank" in types:
        types.remove("blank")

    # If a column contains integer and float values, make it float
    if len(types) == 2 and "integer" in types and "float" in types:
        types.remove("integer")

    return types


def determine_type(value: str) -> str:
    if value == "":
        return "blank"

    if value in NULL_VALUES:
        return value

    try:
        int(value)
        return "integer"
    except ValueError:
        try:
            float(value)
            return "float"
        except ValueError:
            return "string"


def convert_data_type(row):
    data_type = row["Field Type"]
    enum = row["Choices, Calculations, OR Slider Labels"]

    parsed_data = parse_integer_enums(enum)
    if len(parsed_data) > 0:
        return "integer"

    parsed_data = parse_string_enums(enum)
    if len(parsed_data) > 0:
        return "string"

    # find enumeration with text values
    if "|" in enum:
        return "string"

    if data_type in [
        "text",
        "list",
        "url",
        "sequence",
        "category",
        "yesno",
        "radio",
        "dropdown",
        "checkbox",
        "zipcode",
    ]:
        return "string"

    return data_type


def parse_integer_enums(enum):
    # Example: 1, Male | 2, Female | 3, Intersex | 4, None of these describe me
    matches = re.findall(enum_pattern_int, enum)
    parsed_data = [(int(match[0]), match[1].strip()) for match in matches]
    return parsed_data


def parse_string_enums(enum):
    # Example: AL, Alabama | AK, Alaska | AS, American Samoa
    matches = re.findall(enum_pattern_str, enum)
    parsed_data = [(match[0].strip(), match[1].strip()) for match in matches]
    return parsed_data


def parse_value_enums(enum):
    # Example: aptamer | antibody | antigen | molecular beacon | nanobody | primer | receptor | DNA-oligonucleotide | analyte_binding_protein
    values = enum.split("|")
    values = [value.strip() for value in values]
    if len(values) > 0 and len(values[0]) > 0:
        return values
    else:
        return []


def check_enums(data_file, dict_file, error_messages):
    data = pd.read_csv(
        data_file, dtype=str, keep_default_na=False, skip_blank_lines=False
    )
    
    error = False
    
    # Get the allowed values for enumerated types
    allowed_values = get_allowed_values(dict_file)

    # Check data file columns with enumerated values
    for column, enum_values in allowed_values.items():
        column_values = data[column].unique()
        # Empty values are ok, remove them
        column_values = set(filter(None, column_values))
        enum_values = set(enum_values)
        mismatches = column_values - enum_values

        if len(mismatches) > 0:
            message = f"Invalid enumerated value(s): {mismatches} in column {column}"
            error = append_error(message, data_file, error_messages)
            #error = True

    # TODO: for enums, check if the Field Type is correct?
    return error


def get_allowed_values(dict_file):
    allowed_values = dict()
    dictionary = pd.read_csv(
        dict_file, dtype=str, keep_default_na=False, skip_blank_lines=False
    )
    dictionary = dictionary[dictionary["Choices, Calculations, OR Slider Labels"] != ""]

    # Create a dictionary of Variable name and enumerated values
    if dictionary.shape[0] > 0:
        dictionary["values"] = dictionary.apply(get_enum_values, axis=1)  
        allowed_values = dictionary.set_index("Variable / Field Name")["values"].to_dict()
    
    return allowed_values


def get_enum_values(row):
    enum = row["Choices, Calculations, OR Slider Labels"]

    parsed_data = parse_integer_enums(enum)
    if len(parsed_data) > 0:
        # Extract the integer values
        values = [str(item[0]) for item in parsed_data]
        return values

    parsed_data = parse_string_enums(enum)
    if len(parsed_data) > 0:
        # Extract the integer values
        values = [str(item[0]) for item in parsed_data]
        return values

    parsed_data = parse_value_enums(enum)
    if len(parsed_data) > 0:
        # Extract string values
        return parsed_data

    # TODO this should never happen - error message?
    return []
    
    
def update_meta_data(
    meta_file, meta_output_file, meta_data_template_path, data_file, error_messages
):
    error = False

    # Get metadata template
    prefix = extract_prefix(os.path.basename(meta_file))
    template_file = os.path.join(meta_data_template_path, f"{prefix}_TEMPLATE_META.csv")
    
    if not os.path.exists(template_file):
        message = f"Metadata template file {template_file} not found"
        error = append_error(message, meta_file, error_messages)
        return error
    meta_template = pd.read_csv(template_file)

    # Get specimen type from data file
    specimen_type_used = extract_speciment_type(data_file)

    # Extract data file title
    meta_data = pd.read_csv(meta_file)
    description = meta_data[
        meta_data["Field Label"] == "datafile_names - add_additional_rows_as_needed"
    ]

    # There is some inconsistency in the META files. Use Descriptions instead of Description
    data.rename(columns={"Description": "Descriptions"}, inplace=True)

    if description.shape[0] == 0 or not "Descriptions" in description.columns:
        message = "Data file description not found"
        error = append_error(message, meta_file, error_messages)
        #error = True
        return error
    data_file_title = description["Description"].tolist()
    data_file_title = data_file_title[0]

    # Extract timestamp
    timestamp = meta_data[meta_data["Field Label"] == "data_file_creation_dateTime"].copy()
    if timestamp.shape[0] == 0:
        data_file_creation_dateTime = "placeholder"
    else:
        data_file_creation_dateTime = timestamp["Choices"].tolist()[0]

    # Get the SHA256 hash code for the data file
    data_file_sha256_digest = calculate_sha256(data_file)

    # Create additional rows for metadata file
    data_file_name = os.path.basename(data_file)
    data_dictionary_file_name = data_file_name.replace("_DATA_", "_DICT_")
    additional_rows = [
        {"Field": "specimen_type_used", "Value": specimen_type_used},
        {"Field": "data_file_name", "Value": data_file_name},
        {"Field": "data_file_title", "Value": data_file_title},
        {"Field": "data_dictionary_file_name", "Value": data_dictionary_file_name},
        {"Field": "data_file_creation_dateTime", "Value": data_file_creation_dateTime},
        {"Field": "data_file_sha256_digest", "Value": data_file_sha256_digest},
    ]

    additional_data = pd.DataFrame(additional_rows)
    metadata = pd.concat([meta_template, additional_data])

    metadata.to_csv(meta_output_file, index=False)

    return error


def calculate_sha256(file_path):
    sha256_hash = hashlib.sha256()

    with open(file_path, "rb") as f:
        # Read and update hash string value in blocks of 4K
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)

    return sha256_hash.hexdigest()


def extract_speciment_type(data_file):
    data = pd.read_csv(
        data_file, dtype=str, keep_default_na=False, skip_blank_lines=False
    )
    specimens_used = set()
    for specimen in SPECIMEN_COLUMNS:
        specimens_used = specimens_used.union(extract_unique_column_values(data, specimen))

    return ",".join(specimens_used)


def extract_unique_column_values(df, column):
    if column in df.columns:
        specimens = set(df[column].unique())
        # aggregate wastewater sample type to "wastewater"
        if column == "sample_type":
            if "composite" in speciment or "grap" in specimen:
                speciments = {"wastewater"}
        return specimens
    else:
        return set()


def extract_prefix(filename):
    # Split the filename by the underscore character
    parts = filename.split("_", 3)  # Split into at most 4 parts

    # Join the first three parts to form the prefix
    prefix = "_".join(parts[:3])

    return prefix


def fix_dict_columns(dictionary, field_names):
    # Add missing columns
    for field in field_names:
        if not field in dictionary.columns:
            dictionary[field] = ""

    # Reorder the columns
    dictionary = dictionary[field_names]
    return dictionary



    
def data_dict_matcher(data_file, dict_file, error_file, error_messages):
    data = pd.read_csv(
        data_file, dtype=str, keep_default_na=False, skip_blank_lines=False
    )
    dictionary = pd.read_csv(
        dict_file, dtype=str, keep_default_na=False, skip_blank_lines=False
    )

    # remove extra data elements in the dictionary that not present in the data file
    data_fields = set(data.columns)
    dictionary = dictionary[dictionary["Variable / Field Name"].isin(data_fields)]

    # check for missing data element (data fields that are not present in the dictionary)
    data_elements = set(dictionary["Variable / Field Name"].tolist())
    missing_data_elements = list(data_fields - data_elements)

    error = False
    if len(missing_data_elements) > 0:
        message = f"DICT file is missing data elements: {missing_data_elements}"
        error = append_error(message, dict_file, error_messages)
        #error = True
        # add placeholders for the missing data elements
        dictionary = add_missing_data_elements(dictionary, missing_data_elements)
        message = f"Added DICT missing data elements in _tofix file: {missing_data_elements}, fill in fields"
        # TODO if the missing data element is part of the harmonized RADx-rad data elements, it doesn't need to be filled in here!!!1
        error = append_warning(message, dict_file, error_messages)
        tofix_file = get_tofix_file(dict_file)
        dictionary.to_csv(tofix_file, index=False)
    else:
        # reorder the dictionary data elements to match the order in the data file
        dictionary = reorder_data_dictionary(dictionary, list(data.columns))
        output_file = get_output_file(dict_file)
        dictionary.to_csv(output_file, index=False)
        error_messages = update_error_file(error_file, dict_file, error_messages)

    return error


def add_missing_data_elements(dictionary, missing_data_elements):
    # create a new row for each missing data element
    new_rows = []
    for data_element in missing_data_elements:
        new_data_element = dict()
        for column in dictionary.columns:
            if column == "Variable / Field Name":
                new_data_element[column] = data_element
            else:
                new_data_element[column] = ""
        new_rows.append(new_data_element)

    # append the new rows to the dictionary
    new_rows_df = pd.DataFrame(new_rows)
    dictionary = pd.concat([dictionary, new_rows_df], ignore_index=True)

    return dictionary


def reorder_data_dictionary(dictionary, data_fields):
    # Convert the "Variable / Field Name" column to a categorical type with the specified order
    dictionary["Variable / Field Name"] = pd.Categorical(
        dictionary["Variable / Field Name"], categories=data_fields, ordered=True
    )

    # Sort the DataFrame by the "Variable / Field Name" column
    dictionary = dictionary.sort_values("Variable / Field Name").reset_index(drop=True)

    # Convert the "Variable / Field Name" column back to a string
    dictionary["Variable / Field Name"] = dictionary["Variable / Field Name"].astype(
        str
    )

    return dictionary


def update_dict_file(dict_file, dict_output_file):
    dictionary = pd.read_csv(
        dict_file, dtype=str, keep_default_na=False, skip_blank_lines=False
    )
    # Some wastewater projects used Units field instead of Unit field
    dictionary = dictionary.rename(columns={"Units": "Unit"})
    
    # Fill in empty Section Header and CDE Reference columns
    dictionary["Section Header"] = dictionary["Section Header"].replace("", "Project specific")
    dictionary["CDE Reference"] = dictionary["CDE Reference"].replace("", "Depositor")
    
    dictionary.to_csv(dict_output_file, index=False)


def set_cardinality(data_type):
    if data_type == "list":
        return "multiple"
    else:
        return "single"


def convert_data_type_new(row):
    data_type = row["Datatype"]
    enum = row["Enumeration"]
    
    parsed_data = parse_integer_enums(enum)
    if len(parsed_data) > 0:
        return "integer"

    parsed_data = parse_string_enums(enum)
    if len(parsed_data) > 0:
        return "string"
    
    # find enumeration with text values
    if "|" in enum:
        return "string"
    
    if data_type in ["text", "list", "url", "sequence", "category", "yesno", "radio", "dropdown", "checkbox", "zipcode"]:
        return "string"
    
    return data_type


def convert_enumeration(enum):
    
    # parse integer and string encoded enumerations
    parsed_data = parse_integer_enums(enum) + parse_string_enums(enum)
    
    if parsed_data and len(parsed_data) > 0:
        enums = []
        for value, label in parsed_data:
            enums.append(f'"{value}"=[{label}]')
            
        return " | ".join(enums)
    
    # parse simple value enumerations. Example: IgA | IgG | IgM
    if "|" in enum:
        enums = []
        values = enum.split("|")
        for value in values:
            value = value.strip()
            enums.append(f'"{value}"=[{value}]')

        return " | ".join(enums)
            
    
    return ""


def extract_urls(input_string):
    # Define a regular expression pattern for URLs
    url_pattern = re.compile(
        r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
    )
    # Find all matches of the pattern in the input string
    urls = url_pattern.findall(input_string)
    # Return the list of URLs
    return urls

def count_urls(input_string):
    return len(extract_urls(input_string))
    
    
def check_provenance(dict_file, error_messages):
    dictionary = pd.read_csv(
        dict_file, dtype=str, keep_default_na=False, skip_blank_lines=False
    )

    # Check number of URLs in the CDE Reference column. There must be no more than one URL.
    dictionary["url_count"] = dictionary["CDE Reference"].apply(count_urls)
    dictionary.query("url_count > 1", inplace=True)

    error = False
    if dictionary.shape[0] > 0:
        print("check_provenance:")
        print(dictionary.head().to_string())
        message = f"CDE Reference column contains multiple URLs. Only one URL is allowed."
        error = append_error(message, dict_file, error_messages)
        error = True

    return error

    
def split_provenance(provenance):
    see_also = ""
    if len(provenance) > 0 and "|" in provenance:
        provenance, see_also = provenance.split("|", maxsplit=1)
        # If URL is in provenance, switch places with see_also
        if count_urls(provenance) == 1:
            see_also, provenance = provenance, see_also

    return pd.Series([provenance, see_also])


def convert_dict(dict_file, dict_output_file):
    dictionary = pd.read_csv(
        dict_file, dtype=str, keep_default_na=False, skip_blank_lines=False
    )
    dictionary.rename(columns=column_map, inplace=True)
    dictionary = dictionary[["Id", "Label", "Section", "Datatype", "Unit", "Enumeration", "Notes", "Provenance"]].copy()

    # Split the 'Provenance' column into two columns (URLs go into the SeeAlso column)
    dictionary[["Provenance", "SeeAlso"]] = dictionary["Provenance"].apply(split_provenance)

    dictionary["Cardinality"] = dictionary["Datatype"].apply(set_cardinality)
    dictionary["Datatype"] = dictionary.apply(convert_data_type_new, axis=1)
    dictionary["Enumeration"] = dictionary["Enumeration"].apply(convert_enumeration)

    dictionary = dictionary[["Id", "Label", "Section", "Cardinality", "Datatype", "Unit", "Enumeration", "Notes", "Provenance", "SeeAlso"]]
    dictionary.to_csv(dict_output_file, index=False)
