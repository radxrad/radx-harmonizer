#!/usr/bin/python3
import os
import sys
import glob
import pathlib
import subprocess
import traceback
import re

import hashlib
import pandas as pd


# Mandatory columns in dictionary files
MANDATORY_COLUMNS = {
    "Variable / Field Name",
    "Field Label",
    "Field Type",
}

# Mandatory and optional columns in dictionary files
USABLE_COLUMNS = {
    "Variable / Field Name",
    "Section Header",
    "Field Type",
    "Field Label",
    "Choices, Calculations, OR Slider Labels",
    "Field Note",
    "Unit",
    "CDE Reference",
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

# Null values to be replaced by empty string
NULL_VALUES = ["N/A", "NA", "NULL", "NaN", "None", "n/a", "nan", "null"]

# Example: 1, Male | 2, Female | 3, Intersex | 4, None of these describe me
ENUM_PATTERN_INT = r"(\d+),\s*([^|]+)\s*(?:\||$)"

# Example: AL, Alabama | AK, Alaska | AS, American Samoa
ENUM_PATTERN_STR = r"([A-Z]+),\s*([^|]+)\s*(?:\||$)"

# Map of RADx-rad dictionary columns to NIH Data Hub format
COLUMN_MAP = {
    "Variable / Field Name": "Id",
    "Section Header": "Section",
    "Field Type": "Datatype",
    "Field Label": "Label",
    "Choices, Calculations, OR Slider Labels": "Enumeration",
    "Field Note": "Notes",
    "Unit": "Unit",
    "CDE Reference": "Provenance",
}

# Field names that contain specimen information
SPECIMEN_COLUMNS = [
    "specimen_type",
    "virus_sample_type",
    "sample_media",
    "sample_type",
    "covid_test_specimen_type",
]

COVID_TEST_SPECIMEN_TYPES = {
    "1": "Anterior nasal swab",
    "2": "Mid-turbinate nasal swab",
    "3": "Nasopharyngeal swab",
    "4": "Oropharyngeal swab",
    "5": "Nasal lavage",
    "6": "Saliva",
    "7": "Sputum",
    "8": "Whole blood",
    "9": "Plasma",
    "10": "Stool",
    "90": "Other",
}


# Standard units
# https://github.com/bmir-radx/radx-data-dictionary-specification/blob/main/radx-data-dictionary-specification.md#field-unit
# Units for lab tests
# https://www.cdc.gov/cliac/docs/addenda/cliac0313/13A_CLIAC_2013March_UnitsOfMeasure.pdf
STANDARD_UNITS = {
    "pound": "lb",
    "kilograms": "kg",
    "percent": "%",
    "mMol/L": "mmol/L",
    # "gm/dL": "g/dL", check rad_023_610-01
    # "gms.dL": "g/dL", check
    "beats per minute": "beats/min",
    "breaths per minute": "breaths/min",
    "Seconds(s)": "s",
    "seconds": "s",
    "sec": "s",
    "minutes": "min",
    "days": "d",
    "months": "month",
    "years, to tenth percent": "year",
    "mm/hr": "mm/h",  # check
    "Celsius": "°C",
    "Celcius": "°C",
    "celsius": "°C",
    "Â°C": "°C",
    "centimeters": "cm",
    "nanometers": "nm",
    "mmHg": "mm Hg",
    "free text": "",
    "N/A": "",
}

RADX_RAD_TO_RADX_GLOBAL = {
    "study_id": "nih_record_id",
    "race": "nih_race",
    "ethnicity": "nih_ethnicity",
    "age": "nih_age",
    "sex": "nih_sex",
    "education": "nih_education_yrs",
    "zip": "nih_zip",
    "employment": "nih_employment",
    "insurance": "nih_insurance",
    "deaf": "nih_deaf",
    "blind": "nih_blind",
    "memory_dis": "nih_memory",
    "walking_climbing_dis": "nih_walk_climb",
    "dress_bathe_dis": "nih_dress_bathe",
    "errand_dis": "nih_errand",
    "vaping": "nih_vaping_yn",
    "nicotine": "nih_nicotine_yn",
    "alcohol_use": "nih_alcohol_yn",
    "asthma": "nih_asthma",
    "cancer": "nih_cancer",
    "cardiovascular_disease": "nih_cardiovascular_disease",
    "chronic_kidney_disease": "nih_chronic_kidney_disease",
    "chronic_lung_disease": "nih_chronic_lung_disease",
    "diabetes": "nih_diabetes",
    "hypertension": "nih_hypertension",
    "immunosuppressive_conditio": "nih_immunosuppressive_condition",
    "serious_mental_illness": "nih_serious_mental_illness",
    "sickle_cell_disease": "nih_sickle_cell_disease",
    "pregnancy_status": "nih_pregnancy",
    "cough": "nih_cough",
    "fever": "nih_fever_chills1",  # combine
    "chills": "nih_fever_chills2",  # combine
    "shortness_of_breath_or_dif": "nih_diff_breath",
    "headache": "nih_headache",
    "muscle_ache": "nih_muscle_ache",
    "new_loss_of_taste_or_smell": "nih_olfactory",
    "excessive_fatigue": "nih_fatigue",
    "nausea_vomiting": "nih_nausea_vomiting_diarrhea1",  # combine
    "diarrhea": "nih_nausea_vomiting_diarrhea2",  # combine
    "abdominal_pain": "nih_abdom_pain",
    "skin_rash": "nih_skin_rash",
    "conjunctivitis": "nih_conjunctivitis",
    "height_feet": "nih_height1",
    "height_inches": "nih_height2",  # height_feet * 12 + height_inches
    "weight_lbs": "nih_weight",
    "health_status": "nih_health_status",
}

RADX_RAD_COMBINED_TO_RADX_GLOBAL = {
    "fever": "nih_fever_chills",
    "chills": "nih_fever_chills",
    "nausea_vomiting": "nih_nausea_vomiting_diarrhea",
    "diarrhea": "nih_nausea_vomiting_diarrhea",
    "height_feet": "nih_height",
    "height_inches": "nih_height",
    "weight_lbs": "nih_weight",
}

# Mappings that are not one-to-one
RADX_RAD_TO_RADX_GLOBAL_MAPPINGS = {
    "nih_race": {"1": "0", "2": "2", "3": "1", "4": "3", "5": "4", "6": "97", "": ""},
    "nih_sex": {"1": "1", "2": "0", "3": "2", "4": "96", "": ""},
    "nih_employment": {"1": "0", "2": "97", "3": "1", "": ""},
    "nih_insurance": {"1": "1", "2": "2", "3": "0", "": ""},
    "nih_health_status": {"1": "0", "2": "1", "3": "2", "4": "3", "5": "4", "": ""},
}

# This map contains the combination rules for two yes/no columns:
# fever+chills and nausea_vomiting+diarrhea
# The values in the columns can be be 1 (yes), 2 (no), or "" (bank).
# The key in this map is the concatenated value of the two columns.
# The value in this map is the result of the combination
YES_NO_MAPPINGS = {
    "11": "1",  ##yes + yes -> yes
    "10": "1",  # yes + no -> yes
    "01": "1",  # no + yes -> yes
    "00": "0",  # no + no -> no
    "1": "1",  # yes + blank or blank + yes -> yes
    "0": "0",  # no + blank or blank + no -> no
    "": "",  # blank + blank -> blank
}


def append_error(message, filename, error_messages):
    error_messages.append(
        {
            "severity": "ERROR",
            "filename": os.path.basename(filename),
            "message": message,
        }
    )
    return True


def append_warning(message, filename, error_messages):
    error_messages.append(
        {
            "severity": "WARN",
            "filename": os.path.basename(filename),
            "message": message,
        }
    )
    return True


def save_error_messages(error_file, error_messages):
    errors = pd.DataFrame(error_messages)
    errors.sort_values("filename", inplace=True)
    errors.to_csv(error_file, index=False)


def handle_errors_and_continue(error_file, error_messages):
    if len(error_messages) > 0:
        save_error_messages(error_file, error_messages)
        print(f" - failed: {len(error_messages)} errors")
        return True
    return False


def run_command(command):
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    stdout, stderr = process.communicate()
    return stdout.decode('utf-8'), stderr.decode('utf-8')


def get_directories(include, exclude, data_dir):
    all_dirs = glob.glob(os.path.join(data_dir, "rad_*_*-*"))

    if include:
        include_dirs = include.split(",")
        dir_list = [f for f in all_dirs if os.path.basename(f) in include_dirs]
        if len(dir_list) != len(include_dirs):
            print(
                f"ERROR: Some or all of the following projects don't exist: {include_dirs}"
            )
            sys.exit(-1)
        return dir_list
    if exclude:
        exclude_dirs = exclude.split(",")
        dir_list = [f for f in all_dirs if os.path.basename(f) in exclude_dirs]
        if len(dir_list) != len(exclude_dirs):
            print(
                f"ERROR: Some or all of the following projects don't exist: {exclude_dirs}"
            )
            sys.exit(-1)
        return [f for f in all_dirs if os.path.basename(f) not in exclude_dirs]

    return []


def confirm_rest(dir_name="work"):
    print()
    print(f"-reset will remove all files in the {dir_name} directory!")
    confirmation = (
        input(
            f"Are you sure you want to reset the {dir_name} directory? Type 'yes' to confirm: "
        )
        .strip()
        .lower()
    )
    return confirmation == "yes"


def file_is_missing(directory, postfix, error_messages):
    all_files = set(glob.glob(os.path.join(directory, "*")))
    data_files = set(
        glob.glob(os.path.join(directory, f"rad_*_*-*_DATA_{postfix}.csv"))
    )
    dict_files = set(
        glob.glob(os.path.join(directory, f"rad_*_*-*_DICT_{postfix}.csv"))
    )
    meta_files = set(
        glob.glob(os.path.join(directory, f"rad_*_*-*_META_{postfix}.csv"))
    )

    any_error = False
    # # Check for files that don't match the file naming convention
    extra_files = all_files - data_files - dict_files - meta_files
    for extra_file in extra_files:
        message = "Unrecognized file name"
        error = append_error(message, extra_file, error_messages)
        any_error = any_error or error

    # Check that the number of DATA, DICT, and META files is the same
    if len(data_files) != len(dict_files) or len(data_files) != len(meta_files):
        message = "DATA, DICT, META file mismatch"
        error_messages.append(
            {"severity": "ERROR", "filename": directory, "message": message}
        )
        any_error = True

    for data_file in data_files:
        # Check for missing DICT files
        dict_file = data_file.replace(f"_DATA_{postfix}.csv", f"_DICT_{postfix}.csv")
        if not dict_file in dict_files:
            message = "DICT file missing"
            error = append_error(message, dict_file, error_messages)
            any_error = any_error or error

        # Check for missing META files
        meta_file = data_file.replace(f"_DATA_{postfix}.csv", f"_META_{postfix}.csv")
        if not meta_file in meta_files:
            message = "META file missing"
            error = append_error(message, meta_file, error_messages)
            any_error = any_error or error

    return any_error


def file_is_missing_in_work_directory(directory, postfix, error_messages):
    all_files = set(glob.glob(os.path.join(directory, "*")))
    data_files = set(glob.glob(os.path.join(directory, "rad_*_*-*_*_DATA*.csv")))
    dict_files = set(glob.glob(os.path.join(directory, "rad_*_*-*_*_DICT*.csv")))
    meta_files = set(glob.glob(os.path.join(directory, "rad_*_*-*_*_META*.csv")))

    any_error = False
    # Check for files that don't match the file naming convention
    extra_files = all_files - data_files - dict_files - meta_files
    for extra_file in extra_files:
        # Ignore lock file
        if "lock.txt" in extra_file:
            continue
        message = "Unrecognized file name"
        error = append_error(message, extra_file, error_messages)
        any_error = any_error or error

    # Check that the number of DATA, DICT, and META files is the same
    if len(data_files) != len(dict_files) or len(data_files) != len(meta_files):
        message = "DATA, DICT, META file mismatch"
        error_messages.append(
            {"severity": "ERROR", "filename": directory, "message": message}
        )
        any_error = True

    for data_file in data_files:

        if postfix in data_file:
            dict_file = data_file.replace(
                f"_DATA_{postfix}.csv", f"_DICT_{postfix}.csv"
            )
            # Check for missing DICT files
            if not dict_file in dict_files:
                message = "DICT file missing"
                error = append_error(message, dict_file, error_messages)
                any_error = any_error or error

            # Check for missing META files
            meta_file = data_file.replace(
                f"_DATA_{postfix}.csv", f"_META_{postfix}.csv"
            )
            if not meta_file in meta_files:
                message = "META file missing"
                error = append_error(message, meta_file, error_messages)
                any_error = any_error or error
        else:
            # Check for missing DICT files
            dict_file = data_file.replace("_DATA.csv", "_DICT.csv")
            if not dict_file in dict_files:
                message = "DICT file missing"
                error = append_error(message, dict_file, error_messages)
                any_error = any_error or error

            # Check for missing META files
            meta_file = data_file.replace("_DATA.csv", "_META.csv")
            if not meta_file in meta_files:
                message = "META file missing"
                error = append_error(message, meta_file, error_messages)
                any_error = any_error or error

    return any_error


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
        return error

    data.rename(columns={"Description": "Descriptions"}, inplace=True)

    columns = data.columns

    if len(columns) != 3:
        message = f"Metadata file has {len(columns)} columns, 3 columns are required"
        error = append_error(message, filename, error_messages)

    if error:
        return error

    required_columns = ["Field Label", "Choices", "Descriptions"]

    any_error = False
    # Check column names
    for i, expected_column in enumerate(required_columns):
        if columns[i] != expected_column:
            message = f"{expected_column} column missing"
            error = append_error(message, filename, error_messages)
            any_error = any_error or error

    if any_error:
        return any_error

    # check the number of data files
    filenames = data[data["Field Label"] == "number_of_datafiles_in_this_package"]
    if filenames.shape[0] != 1:
        message = "Row 'number_of_datafiles_in_this_package' is missing"
        error = append_error(message, filename, error_messages)

    if error:
        return error

    num_files = filenames["Choices"].tolist()
    if num_files[0] != "1":
        message = f"number_of_datafiles_in_this_package is {num_files[0]}, it must be 1"
        error = append_error(message, filename, error_messages)
        any_error = any_error or error

    # check data file name
    filenames = data[
        data["Field Label"] == "datafile_names - add_additional_rows_as_needed"
    ]
    if filenames.shape[0] != 1:
        message = "Row 'datafile_names - add_additional_rows_as_needed' is missing"
        error = append_error(message, filename, error_messages)

    if error:
        return error

    data_file = os.path.basename(filename).replace("_META_", "_DATA_")
    data_files = filenames["Choices"].tolist()
    if data_files[0] != data_file:
        message = f"Data file name: {data_files[0]} doesn't match"
        error = append_error(message, filename, error_messages)
        any_error = any_error or error

    description = filenames["Descriptions"].tolist()
    description = description[0]
    if description == "":
        message = "Data file description is missing"
        error = append_error(message, filename, error_messages)
        any_error = any_error or error

    return any_error


def check_origcopy_meta_file(filename, error_messages):
    error = False
    try:
        meta = pd.read_csv(
            filename,
            encoding="utf8",
            dtype=str,
            keep_default_na=False,
            skip_blank_lines=False,
        )
    except Exception:
        message = f"Invalid csv file: {traceback.format_exc().splitlines()[-2]}"
        error = append_error(message, filename, error_messages)
        return error

    any_error = False

    data_file = os.path.basename(filename).replace("_META_", "_DATA_")
    meta_data_file = meta.query("Field == 'data_file_name'").copy()
    data_files = meta_data_file["Value"].tolist()
    if data_files[0] != data_file:
        message = f"Data file name: {data_files[0]} doesn't match"
        error = append_error(message, filename, error_messages)
        any_error = any_error or error

    dict_file = os.path.basename(filename).replace("_META_", "_DICT_")
    meta_dict_file = meta.query("Field == 'data_dictionary_file_name'").copy()
    dict_files = meta_dict_file["Value"].tolist()
    if dict_files[0] != dict_file:
        message = f"Dictionary file name: {dict_files[0]} doesn't match"
        error = append_error(message, filename, error_messages)
        any_error = any_error or error

    return any_error


def is_not_utf8_encoded(filename, error_messages):
    error = False
    try:
        pd.read_csv(
            filename,
            encoding="utf8",
            dtype=str,
            low_memory=False,
            skip_blank_lines=False,
        )
    except Exception:
        message = f"Not utf-8 encoded or invalid csv file: {traceback.format_exc().splitlines()[-1]}"
        error = append_error(message, filename, error_messages)

    return error


def check_column_names(data, filename, error_messages):
    any_error = False
    if len(data.columns) != data.shape[1]:
        message = "Number of columns in header do not match the data"
        error = append_error(message, filename, error_messages)
        any_error = any_error or error
    for col in data.columns:
        col_stripped = col.strip()
        if col_stripped != col:
            message = f"column header: '{col}' contains spaces"
            error = append_error(message, filename, error_messages)
            any_error = any_error or error
        if col_stripped == "":
            message = "Empty column name"
            error = append_error(message, filename, error_messages)
            any_error = any_error or error
        if "Unnamed" in col:
            message = f"Unnamed column: {col}"
            error = append_error(message, filename, error_messages)
            any_error = any_error or error

    return any_error


def remove_empty_rows_cols(filename, error_messages):
    data = pd.read_csv(
        filename,
        encoding="utf8",
        dtype=str,
        keep_default_na=False,
        skip_blank_lines=False,
    )

    # Remove leading and trailing whitespace
    if pd.__version__ < "2.1.0":
        data = data.applymap(lambda x: x.strip())
    else:
        data = data.map(lambda x: x.strip())

    # Identify rows with all empty strings
    empty_row_mask = data.eq("").all(axis=1)
    data = data[~empty_row_mask]

    # Identify empty columns
    empty_cols = [col for col in data.columns if data[col].eq("").all()]

    # Remove empty columns that are not in the exclusion list
    cols_to_drop = [col for col in empty_cols if col not in USABLE_COLUMNS]
    data = data.drop(columns=cols_to_drop)

    # Drop all rows and columns that contain all NA values
    data.dropna(axis="rows", how="all", inplace=True)
    data.dropna(axis="columns", how="all", inplace=True)

    # Remove Unnamed columns
    data = remove_unnamed_columns(data)

    error = check_column_names(data, filename, error_messages)
    if error:
        return error

    data.to_csv(filename, index=False)
    return False


def standardize_units(filename):
    df = pd.read_csv(
        filename,
        encoding="utf8",
        dtype=str,
        keep_default_na=False,
        skip_blank_lines=False,
    )

    # Create a list of columns that end with '_unit'
    unit_columns = [col for col in df.columns if col.endswith("_unit")]
    # Standardize '_unit' columns
    df[unit_columns] = df[unit_columns].replace(STANDARD_UNITS)

    df.to_csv(filename, index=False)


def remove_spaces_from_header(filename):
    df = pd.read_csv(
        filename,
        encoding="utf8",
        dtype=str,
        keep_default_na=False,
        skip_blank_lines=False,
    )

    # has_spaces = any(col != (col_stripped := col.strip()) for col in df.columns)
    has_spaces = any(col != col.strip() for col in df.columns)

    if has_spaces:
        df.rename(
            columns={col: col.strip() for col in df.columns if col != col.strip()},
            inplace=True,
        )
        df.to_csv(filename, index=False)


def remove_unnamed_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove columns with 'Unnamed' in their name from a Pandas DataFrame.

    Parameters:
    df (pd.DataFrame): The input DataFrame.

    Returns:
    pd.DataFrame: The DataFrame with 'Unnamed' columns removed.
    """
    # Identify columns to drop
    columns_to_drop = [col for col in df.columns if "Unnamed" in col]

    # Drop the identified columns
    df_cleaned = df.drop(columns=columns_to_drop)

    return df_cleaned


def is_newer(filename1, filename2):
    # the second file doesn't exist yet
    if not os.path.isfile(filename2):
        return True
    # check if the first file is newer than the second file
    return os.path.getmtime(filename1) > os.path.getmtime(filename2)


def save_error_file(error_messages, error_file):
    df = pd.DataFrame(error_messages)
    if len(df) > 0:
        df.to_csv(error_file, index=False)


def get_num_empty_rows(df, field_name):
    df_empty = df[df[field_name] == ""].copy()
    return int(df_empty.shape[0])


def check_dict(filename, error_messages):
    df = pd.read_csv(
        filename,
        encoding="utf8",
        dtype=str,
        keep_default_na=False,
        skip_blank_lines=False,
    )

    # Find missing mandatory columns
    columns = set(df.columns)
    missing_columns = MANDATORY_COLUMNS - columns

    error = False
    if len(missing_columns) > 0:
        message = f"Missing columns: {missing_columns}"
        error = append_error(message, filename, error_messages)

    # Find unexpected columns
    unexpected_columns = columns - ALL_COLUMNS
    if len(unexpected_columns) > 0:
        message = f"Unexpected columns: {unexpected_columns}, either rename or delete these columns."
        append_error(message, filename, error_messages)
        error = True

    return error


def fix_units(filename):
    df = pd.read_csv(
        filename,
        encoding="utf8",
        dtype=str,
        keep_default_na=False,
        skip_blank_lines=False,
    )
    if "Units" in df.columns:
        df.rename(columns={"Units": "Unit"}, inplace=True)
        df.to_csv(filename, index=False)


def check_missing_values(filename, error_messages):
    df = pd.read_csv(
        filename,
        encoding="utf8",
        dtype=str,
        keep_default_na=False,
        skip_blank_lines=False,
    )

    # check for missing values in the required columns
    any_error = False
    for field_name in MANDATORY_COLUMNS:
        num_empty_rows = get_num_empty_rows(df, field_name)
        if num_empty_rows > 0:
            message = f"Column: `{field_name}` has {num_empty_rows} empty values out of {df.shape[0]} rows"
            error = append_error(message, filename, error_messages)
            any_error = any_error or error

    return any_error


def check_field_types(filename, error_messages):
    df = pd.read_csv(
        filename,
        encoding="utf8",
        dtype=str,
        keep_default_na=False,
        skip_blank_lines=False,
    )
    field_types = set(df["Field Type"].unique())
    invalid_field_types = field_types - ALLOWED_TYPES
    error = False
    if len(invalid_field_types) > 0:
        message = f"Invalid field types: {list(invalid_field_types)}"
        error = append_error(message, filename, error_messages)

    return error


def remove_na(filename):
    data = pd.read_csv(
        filename,
        encoding="utf8",
        dtype=str,
        keep_default_na=False,
        skip_blank_lines=False,
    )
    data.replace(NULL_VALUES, "", inplace=True)
    data.fillna("", inplace=True)
    data.to_csv(filename, index=False)


def check_data_type(data_file, dict_file, error_messages):
    remove_na(data_file)

    data = pd.read_csv(
        data_file,
        encoding="utf8",
        dtype=str,
        keep_default_na=False,
        skip_blank_lines=False,
    )
    dict_types = get_dictionary_data_types(dict_file)
    dict_cardinality = get_dictionary_cardinality(dict_file)

    any_error = False
    for column in list(data.columns):
        # Ignore the "type" column. It is used store temporay data types.
        if column in ("type", "cardinality"):
            continue

        # Check cardinality of data. Only list and checkbox can have multiple values separated by "|"
        data_cardinality = get_column_cardinality(data, column)

        if data_cardinality == "multiple" and dict_cardinality.get(column) == "single":
            message = f"Multiple values are not allowed in column: {column}. Check the data dictionary. Only 'list' and 'checkbox' field types can have multiple values"
            error = append_error(message, data_file, error_messages)
            any_error = any_error or error

        # Check data types
        types = get_column_type(data, column)
        dict_type = dict_types.get(column)
        if len(types) == 1 and not types[0] == dict_type:
            # Some identifier columns have integer values but are declared as strings
            if dict_type == "string" and types[0] == "integer":
                continue
            # Integer values are ok in float columns
            if dict_type == "float" and types[0] == "integer":
                continue
            message = f"Invalid data type in column: {column}: '{dict_type}' in DICT vs. '{types[0]}' in DATA"
            error = append_error(message, data_file, error_messages)
            any_error = any_error or error
        elif len(types) > 1:
            # mixed types are ok if the type in the dictionary is defined as string
            if dict_type != "string":
                message = f"Mixed data types in column: {column}: '{types}' in DATA vs. '{dict_type}' IN DICT"
                error = append_error(message, data_file, error_messages)
                offending_values = get_offending_data_values(data, column)
                message = f"String values found in column {column}: {offending_values}"
                error = append_error(message, data_file, error_messages)
                any_error = any_error or error

    return any_error


def get_dictionary_data_types(dict_file):
    dictionary = pd.read_csv(
        dict_file,
        encoding="utf8",
        dtype=str,
        keep_default_na=False,
        skip_blank_lines=False,
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


def get_column_cardinality(df, fieldname):
    data = df[fieldname].to_list()
    for item in data:
        if "|" in item:
            return "multiple"

    return "single"


def get_offending_data_values(df, fieldname):
    df_string = df[df["type"] == "string"].copy()
    return set(df_string[fieldname].unique())


def determine_type(value: str) -> str:
    # split multi-value values
    if "|" in value:
        types = {determine_type(item) for item in value.split("|")}
        # mixed integers and floats are represented as floats
        if set(types) == {"integer", "float"}:
            types.remove("integer")
        # if there are multiple types, make it a string
        if len(types) > 1:
            return "string"

        return list(types)[0]

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


def get_dictionary_cardinality(dict_file):
    dictionary = pd.read_csv(
        dict_file,
        encoding="utf8",
        dtype=str,
        keep_default_na=False,
        skip_blank_lines=False,
    )
    dictionary["cardinality"] = dictionary.apply(determine_cardinality, axis=1)
    dict_types = dictionary.set_index("Variable / Field Name")["cardinality"].to_dict()

    return dict_types


def determine_cardinality(row):
    data_type = row["Field Type"]

    if data_type in ("list", "checkbox"):
        return "multiple"

    return "single"


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
        "date",
        "time",
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
    matches = re.findall(ENUM_PATTERN_INT, enum)
    parsed_data = [(int(match[0]), match[1].strip()) for match in matches]
    return parsed_data


def parse_string_enums(enum):
    # Example: AL, Alabama | AK, Alaska | AS, American Samoa
    matches = re.findall(ENUM_PATTERN_STR, enum)
    parsed_data = [(match[0].strip(), match[1].strip()) for match in matches]
    return parsed_data


def parse_value_enums(enum):
    # Example: aptamer | antibody | antigen | molecular beacon | nanobody | primer | receptor | DNA-oligonucleotide | analyte_binding_protein
    values = enum.split("|")
    values = [value.strip() for value in values]
    if len(values) > 0 and len(values[0]) > 0:
        return values

    return []


def check_enums(data_file, dict_file, error_messages):
    data = pd.read_csv(
        data_file,
        encoding="utf8",
        dtype=str,
        keep_default_na=False,
        skip_blank_lines=False,
    )

    # Get the allowed values for enumerated types
    allowed_values = get_allowed_values(dict_file)

    # Get a list of fields that have a "List" Field Type
    # (list of values separated by the vertical bar | without spaces.)
    multi_value_fields = get_multi_value_fields(dict_file)

    # Check data file columns with enumerated values
    any_error = False
    for column, enum_values in allowed_values.items():
        column_values = data[column].unique()
        # Empty values are ok, remove them
        column_values = set(filter(None, column_values))

        # Expand lists of enumerated values
        if column in multi_value_fields:
            column_values = expand_column_values(column_values)

        enum_values = set(enum_values)
        mismatches = column_values - enum_values

        if len(mismatches) > 0:
            message = f"Invalid enumerated value(s) in column {column}: {mismatches}"
            error = append_error(message, data_file, error_messages)
            any_error = any_error or error

    # TODO: for enums, check if the Field Type is correct?
    return any_error


def expand_column_values(column_values):
    # For Field Type: "list", split multiple values.
    # Example: analyte_type = viral RNA|human microRNA
    expanded_column_values = {
        val
        for value in column_values
        for val in (value.split("|") if "|" in value else [value])
    }
    return set(expanded_column_values)


def get_allowed_values(dict_file):
    allowed_values = {}
    dictionary = pd.read_csv(
        dict_file,
        encoding="utf8",
        dtype=str,
        keep_default_na=False,
        skip_blank_lines=False,
    )
    dictionary = dictionary[dictionary["Choices, Calculations, OR Slider Labels"] != ""]

    # Create a dictionary of Variable name and enumerated values
    if dictionary.shape[0] > 0:
        dictionary["values"] = dictionary.apply(get_enum_values, axis=1)
        allowed_values = dictionary.set_index("Variable / Field Name")[
            "values"
        ].to_dict()

    return allowed_values


def get_multi_value_fields(dict_file):
    dictionary = pd.read_csv(
        dict_file,
        encoding="utf8",
        dtype=str,
        keep_default_na=False,
        skip_blank_lines=False,
    )
    dictionary = dictionary[
        (dictionary["Field Type"] == "list") | (dictionary["Field Type"] == "checkbox")
    ].copy()
    list_fields = set(dictionary["Variable / Field Name"].to_list())
    return list_fields


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

    # Raise an exception if none of the conditions are met
    raise ValueError(f"Could not parse enum values for the input: {enum}")


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
    meta_template = pd.read_csv(
        template_file,
        encoding="utf8",
        dtype=str,
        keep_default_na=False,
        skip_blank_lines=False,
    )

    # Get specimen type from data file
    specimen_type_used = extract_speciment_type(data_file)

    # Extract data file title
    meta_data = pd.read_csv(
        meta_file,
        encoding="utf8",
        dtype=str,
        keep_default_na=False,
        skip_blank_lines=False,
    )
    description = meta_data[
        meta_data["Field Label"] == "datafile_names - add_additional_rows_as_needed"
    ].copy()

    # There is some inconsistency in the META files. Use Descriptions instead of Description
    description.rename(columns={"Description": "Descriptions"}, inplace=True)

    if description.shape[0] == 0 or not "Descriptions" in description.columns:
        message = "Data file description not found"
        error = append_error(message, meta_file, error_messages)
        return error
    data_file_title = description["Descriptions"].tolist()
    data_file_title = data_file_title[0]

    # Extract timestamp
    timestamp = meta_data[
        meta_data["Field Label"] == "data_file_creation_dateTime"
    ].copy()
    if timestamp.shape[0] == 0:
        data_file_creation_date_time = "placeholder"
    else:
        data_file_creation_date_time = timestamp["Choices"].tolist()[0]

    # Get the SHA256 hash code for the data file
    data_file_sha256_digest = calculate_sha256(data_file)

    # Create additional rows for metadata file
    data_file_name = os.path.basename(data_file)
    data_file_name = data_file_name.replace("_DATA.csv", "_DATA_origcopy.csv")
    data_dictionary_file_name = data_file_name.replace("_DATA_", "_DICT_")
    additional_rows = [
        {"Field": "specimen_type_used", "Value": specimen_type_used},
        {"Field": "data_file_name", "Value": data_file_name},
        {"Field": "data_file_title", "Value": data_file_title},
        {"Field": "data_dictionary_file_name", "Value": data_dictionary_file_name},
        {"Field": "data_file_creation_dateTime", "Value": data_file_creation_date_time},
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


def update_sha256_digest(data_file, metadata_file):
    # Get the SHA256 hash code for the data file
    data_file_sha256_digest = calculate_sha256(data_file)
    metadata = pd.read_csv(metadata_file)
    metadata.loc[metadata["Field"] == "data_file_sha256_digest", "Value"] = (
        data_file_sha256_digest
    )
    metadata.to_csv(metadata_file, index=False)


def extract_speciment_type(data_file):
    data = pd.read_csv(
        data_file,
        encoding="utf8",
        dtype=str,
        keep_default_na=False,
        skip_blank_lines=False,
    )
    specimens_used = set()
    for specimen in SPECIMEN_COLUMNS:
        specimens_used = specimens_used.union(
            extract_unique_column_values(data, specimen)
        )

    return ",".join(specimens_used)


def extract_unique_column_values(df, column):
    if column in df.columns:
        # Aggregate wastewater sample types to "wastewater"
        if column == "sample_type":
            specimens = set(df[column].unique())
            for specimen in specimens:
                if "composite" in specimen or "grap" in specimen:
                    specimens = {"wastewater"}
                    return specimens

        # Decode COVID test specimen type
        if column == "covid_test_specimen_type":
            specimen = list(df[column].unique())
            # Only a single specimen type is allowed here
            specimen = specimen[0]
            # convert the type from integer to string representation
            specimens = set(COVID_TEST_SPECIMEN_TYPES.get(specimen))
            return specimens

        # For all other specimen types
        specimens = set(df[column].unique())

        return specimens

    return set()


def extract_prefix(filename):
    # Split the filename by the underscore character
    parts = filename.split("_", 3)  # Split into at most 4 parts

    # Join the first three parts to form the prefix
    prefix = "_".join(parts[:3])

    return prefix


def data_dict_matcher_new(data_file, dict_file, harmonized_dict, error_messages):
    data = pd.read_csv(
        data_file,
        encoding="utf8",
        dtype=str,
        keep_default_na=False,
        skip_blank_lines=False,
    )
    dictionary = pd.read_csv(
        dict_file,
        encoding="utf8",
        dtype=str,
        keep_default_na=False,
        skip_blank_lines=False,
    )
    # Add missing columns and remove extraneous columns
    dictionary = fix_dictionary_columns(dictionary)

    # Add latest version of the harmonized data elements (they will replace any older versions)
    dictionary = add_harmonized_data_elements(dictionary, harmonized_dict)

    # Remove extra data elements in the dictionary that not present in the data file
    data_fields = set(data.columns)
    primary_key = "Variable / Field Name"
    dictionary = dictionary[dictionary[primary_key].isin(data_fields)]

    # Check for missing data element (data fields that are not present in the dictionary)
    data_elements = set(dictionary[primary_key].tolist())
    missing_data_elements = list(data_fields - data_elements)

    error = False
    if len(missing_data_elements) > 0:
        message = f"DICT file is missing data elements: {missing_data_elements}"
        error = append_error(message, dict_file, error_messages)

        # add placeholders for the missing data elements
        dictionary = add_missing_data_elements(dictionary, missing_data_elements)
        message = (
            f"Added missing data elements {missing_data_elements}, fill in definitions"
        )
        error = append_warning(message, dict_file, error_messages)

    # Drop duplicate data elements
    dictionary.drop_duplicates(subset=primary_key, inplace=True)

    # Reorder the dictionary data elements to match the order in the data file
    dictionary = reorder_data_dictionary(dictionary, primary_key, list(data.columns))
    dictionary.to_csv(dict_file, index=False)

    return error


def fix_dictionary_columns(dictionary):
    # Rename Units to Unit (Units was used by some projects)
    dictionary = dictionary.rename(columns={"Units": "Unit"})

    # Add any missing columns
    actual_cols = set(dictionary.columns)
    required_cols = set(COLUMN_MAP.keys())
    missing_cols = required_cols - actual_cols
    for col in missing_cols:
        dictionary[col] = ""

    # Remove extraneous columns and reorder columns
    dictionary = dictionary[COLUMN_MAP.keys()]

    return dictionary


def add_missing_data_elements(dictionary, missing_data_elements):
    # create a new row for each missing data element
    new_rows = []
    for data_element in missing_data_elements:
        new_data_element = {}
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


def add_harmonized_data_elements(dictionary, harmonized_dict):
    dictionary_harmonized = pd.read_csv(
        harmonized_dict,
        encoding="utf8",
        dtype=str,
        keep_default_na=False,
        skip_blank_lines=False,
    )
    dictionary = pd.concat([dictionary_harmonized, dictionary], ignore_index=True)

    return dictionary


def reorder_data_dictionary(dictionary, primary_key, data_fields):
    # Convert the primary key column to a categorical type with the specified order
    dictionary[primary_key] = pd.Categorical(
        dictionary[primary_key], categories=data_fields, ordered=True
    )

    # Sort the DataFrame by the "Variable / Field Name" column
    dictionary = dictionary.sort_values(primary_key).reset_index(drop=True)

    # Convert the "Variable / Field Name" column back to a string
    dictionary[primary_key] = dictionary[primary_key].astype(str)

    return dictionary


def update_dict_file(dict_file, dict_output_file):
    dictionary = pd.read_csv(
        dict_file,
        encoding="utf8",
        dtype=str,
        keep_default_na=False,
        skip_blank_lines=False,
    )
    # Standardize units
    dictionary["Unit"] = dictionary["Unit"].replace(STANDARD_UNITS)

    # Fill in empty Section Header and CDE Reference columns
    dictionary["Section Header"] = dictionary["Section Header"].replace(
        "", "Project specific"
    )
    dictionary["CDE Reference"] = dictionary["CDE Reference"].replace("", "Depositor")

    dictionary.to_csv(dict_output_file, index=False)


def set_cardinality(data_type):
    if data_type == "list":
        return "multiple"

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
        r"http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+"
    )
    # Find all matches of the pattern in the input string
    urls = url_pattern.findall(input_string)
    # Return the list of URLs
    return urls


def count_urls(input_string):
    return len(extract_urls(input_string))


def check_provenance(dict_file, error_messages):
    dictionary = pd.read_csv(
        dict_file,
        encoding="utf8",
        dtype=str,
        keep_default_na=False,
        skip_blank_lines=False,
    )

    # Check number of URLs in the CDE Reference column. There must be no more than one URL.
    dictionary["url_count"] = dictionary["CDE Reference"].apply(count_urls)
    dictionary.query("url_count > 1", inplace=True)

    error = False
    if dictionary.shape[0] > 0:
        message = (
            "CDE Reference column contains multiple URLs. Only one URL is allowed."
        )
        error = append_error(message, dict_file, error_messages)

    return error


def has_study_id(data_file, dict_file, error_messages):
    error = False
    primary_key = "Variable / Field Name"
    if contains_min_cdes(dict_file, primary_key):
        dictionary = pd.read_csv(
            dict_file,
            encoding="utf8",
            dtype=str,
            keep_default_na=False,
            skip_blank_lines=False,
        )
        field_names = set(dictionary[primary_key].to_list())
        if not "study_id" in field_names:
            message = "Minimum CDEs found: 'study_id' column missing or misnamed"
            error = append_error(message, dict_file, error_messages)
            error = append_error(message, data_file, error_messages)

    return error


def split_provenance(provenance):
    see_also = ""
    if len(provenance) > 0 and "|" in provenance:
        provenance, see_also = provenance.split("|", maxsplit=1)
        # If URL is in provenance, switch places with see_also
        if count_urls(provenance) == 1:
            see_also, provenance = provenance, see_also

    return pd.Series([provenance, see_also])


def extract_example(note):
    match = re.match(r"^(example|examples):\s*(.*)", note, re.IGNORECASE)
    return match.group(2) if match else ""


def remove_example(note):
    return re.split(r"(example|examples):", note, flags=re.IGNORECASE)[0].strip()


def convert_dict(dict_file, tier1_dict_file, tier2_dict_file, dict_output_file):
    dictionary = pd.read_csv(
        dict_file,
        encoding="utf8",
        dtype=str,
        keep_default_na=False,
        skip_blank_lines=False,
    )
    # Select the required fields
    dictionary.rename(columns=COLUMN_MAP, inplace=True)
    dictionary = dictionary[
        [
            "Id",
            "Label",
            "Section",
            "Datatype",
            "Unit",
            "Enumeration",
            "Notes",
            "Provenance",
        ]
    ].copy()

    # Split the 'Provenance' column into two columns (URLs go into the SeeAlso column)
    dictionary[["Provenance", "SeeAlso"]] = dictionary["Provenance"].apply(
        split_provenance
    )

    # Move the examples from the Notes column the "Examples" column
    dictionary["Examples"] = dictionary["Notes"].apply(extract_example)
    dictionary["Notes"] = dictionary["Notes"].apply(remove_example)

    # Add terms column
    dictionary["Terms"] = ""
    # Fill in MissingValueCodes
    dictionary["MissingValueCodes"] = '"-9960"=[Not Entered By Originator]'

    # Convert to new data types
    dictionary["Cardinality"] = dictionary["Datatype"].apply(set_cardinality)
    dictionary["Datatype"] = dictionary.apply(convert_data_type_new, axis=1)
    dictionary["Enumeration"] = dictionary["Enumeration"].apply(convert_enumeration)

    # Order columns
    dictionary = dictionary[
        [
            "Id",
            "Label",
            "Examples",
            "Section",
            "Cardinality",
            "Terms",
            "Datatype",
            "Unit",
            "Enumeration",
            "Notes",
            "MissingValueCodes",
            "Provenance",
            "SeeAlso",
        ]
    ]

    # Read the harmonized RADx-rad tier1 and tier2 data dictionaries
    tier1_dict = pd.read_csv(
        tier1_dict_file,
        encoding="utf8",
        dtype=str,
        keep_default_na=False,
        skip_blank_lines=False,
    )
    tier2_dict = pd.read_csv(
        tier2_dict_file,
        encoding="utf8",
        dtype=str,
        keep_default_na=False,
        skip_blank_lines=False,
    )
    updated_dict = pd.concat([tier1_dict, tier2_dict])

    # Update data elements
    # First, ensure the 'Id' column is set as the index for both DataFrames
    dictionary.set_index("Id", inplace=True)
    updated_dict.set_index("Id", inplace=True)

    # Update rows in df1 with corresponding rows from df2 based on the 'Id' column
    dictionary.update(updated_dict)

    # Reset the index back as a column
    dictionary.reset_index(inplace=True)

    dictionary.to_csv(dict_output_file, index=False)


def contains_min_cdes(dict_file, primary_key):
    dictionary = pd.read_csv(
        dict_file,
        dtype=str,
        encoding="utf8",
        keep_default_na=False,
        skip_blank_lines=False,
    )
    data_elements = set(dictionary[primary_key].to_list())
    min_cdes = set(RADX_RAD_TO_RADX_GLOBAL.keys())
    overlap = min_cdes.intersection(data_elements)
    return len(overlap) > 0


def convert_min_to_global_data(data_file):
    data = pd.read_csv(
        data_file,
        dtype=str,
        encoding="utf8",
        keep_default_na=False,
        skip_blank_lines=False,
    )

    data = data.rename(columns=RADX_RAD_TO_RADX_GLOBAL)
    data = convert_exceptions(data)
    data = convert_height_to_inches(data)
    data = combine_yes_no_cdes(data, "nih_fever_chills")
    data = combine_yes_no_cdes(data, "nih_nausea_vomiting_diarrhea")
    # Add missing value code all emtpy cell
    # https://github.com/bmir-radx/radx-data-dictionary-specification/blob/main/radx-data-dictionary-specification.md#field-missingvaluecodes
    data.replace("", "-9960", inplace=True)

    return data


def combine_yes_no_cdes(data, cde):
    cde1 = f"{cde}1"
    cde2 = f"{cde}2"
    if cde1 in data.columns and not cde2 in data.columns:
        data = data.rename(columns={cde1: cde})
    if cde2 in data.columns and not cde1 in data.columns:
        data = data.rename(columns={cde2: cde})
    if cde1 in data.columns and cde2 in data.columns:
        # Reuse the existing column to avoid rearranging the position of this column
        data[cde1] = data.apply(lambda r: yes_no_combiner(r[cde1], r[cde2]), axis=1)
        data = data.drop(columns=[cde2])
        data = data.rename(columns={cde1: cde})

    return data


def convert_exceptions(data):
    # Apply the mapping if the column exists
    for column, mapping in RADX_RAD_TO_RADX_GLOBAL_MAPPINGS.items():
        if column in data.columns:
            # Special case: for multiple races (separated by "|"), assign 6: Two or more races
            if column == "nih_race":
                data[column] = data[column].apply(lambda x: "6" if "|" in x else x)
            else:
                data[column] = data[column].replace(mapping)

    return data


def yes_no_combiner(cde1, cde2):
    # Create a composite key
    key = cde1 + cde2
    # Return the looked up value
    return YES_NO_MAPPINGS[key]


def convert_height_to_inches(data):
    if "nih_height1" in data.columns and "nih_height2" in data.columns:
        # temporaryly replace emtpy values with zero to enable calculation
        data["nih_height1"] = data["nih_height1"].replace("", "0").fillna("0")
        data["nih_height2"] = data["nih_height2"].replace("", "0").fillna("0")
        data["nih_height2"] = data["nih_height1"].astype(int) * 12 + data[
            "nih_height2"
        ].astype(int)
        data = data.rename(columns={"nih_height2": "nih_height"})
        data = data.drop(columns=["nih_height1"])
        # replace the zero values with empty string
        data["nih_height"] = data["nih_height"].astype(str)
        data["nih_height"] = data["nih_height"].replace("0", "")

    return data


def convert_min_to_global_dict(dict_file, global_harmonized_dict):
    dictionary = pd.read_csv(
        dict_file,
        dtype=str,
        encoding="utf8",
        keep_default_na=False,
        skip_blank_lines=False,
    )
    # Order the columns as in the Global Codebook
    dictionary = dictionary[
        [
            "Id",
            "Label",
            "Examples",
            "Section",
            "Cardinality",
            "Terms",
            "Datatype",
            "Unit",
            "Enumeration",
            "Notes",
            "MissingValueCodes",
            "Provenance",
            "SeeAlso",
        ]
    ]

    # Rename RADx-rad data element to the corresponding RADx global data elements
    dictionary["Id"] = dictionary["Id"].replace(RADX_RAD_COMBINED_TO_RADX_GLOBAL)
    dictionary["Id"] = dictionary["Id"].replace(RADX_RAD_TO_RADX_GLOBAL)
    dictionary = dictionary.drop_duplicates(subset="Id", keep="first")

    global_dictionary = pd.read_csv(
        global_harmonized_dict,
        dtype=str,
        encoding="utf8",
        keep_default_na=False,
        skip_blank_lines=False,
    )

    # Update data elements
    # First, ensure the 'Id' column is set as the index for both DataFrames
    dictionary.set_index("Id", inplace=True)
    global_dictionary.set_index("Id", inplace=True)

    # Update rows in df1 with corresponding rows from df2 based on the 'Id' column
    dictionary.update(global_dictionary)

    # # Reset the index back as a column
    dictionary.reset_index(inplace=True)

    # Fill in MissingValueCodes for depositor created data elements
    dictionary["MissingValueCodes"] = dictionary["MissingValueCodes"].replace(
        "", '"-9960"=[Not Entered By Originator]'
    )

    return dictionary


def global_data_dict_matcher(data_file, dict_file):
    data = pd.read_csv(
        data_file,
        encoding="utf8",
        dtype=str,
        keep_default_na=False,
        skip_blank_lines=False,
    )
    dictionary = pd.read_csv(
        dict_file,
        encoding="utf8",
        dtype=str,
        keep_default_na=False,
        skip_blank_lines=False,
    )
    primary_key = "Id"

    orig_size = dictionary.shape[0]
    # Remove extra data elements in the dictionary that not present in the data file
    data_fields = set(data.columns)
    dictionary = dictionary[dictionary[primary_key].isin(data_fields)]

    # Raise an exception: this should never happen
    if dictionary.shape[0] != orig_size:
        raise ValueError(
            f"Internal error mapping RADx global data elements: {data_file} vs. {dict_file}"
        )

    # Reorder the dictionary data elements to match the order in the data file
    dictionary = reorder_data_dictionary(dictionary, primary_key, list(data.columns))
    dictionary.to_csv(dict_file, index=False)

    return dictionary


def final_consistency_check(
    preorigcopy_dir, origcopy_dir, transformcopy_dir, error_messages
):
    preorigcopies = len(
        glob.glob(os.path.join(preorigcopy_dir, "rad_*_*-*_*_*_preorigcopy.csv"))
    )
    origcopies = len(
        glob.glob(os.path.join(origcopy_dir, "rad_*_*-*_*_*_origcopy.csv"))
    ) + len(glob.glob(os.path.join(origcopy_dir, "rad_*_*-*_*_*_origcopy.json")))

    transformcopies = len(
        glob.glob(os.path.join(transformcopy_dir, "rad_*_*-*_*_*_transformcopy.csv"))
    ) + len(
        glob.glob(os.path.join(transformcopy_dir, "rad_*_*-*_*_*_transformcopy.json"))
    )

    if origcopies % 3 != 0 or origcopies < preorigcopies:
        message = f" - ERROR: invalid number of origcopy files: {origcopies} generated: preorigcopy directory has {preorigcopies} files."
        append_error(message, f"{origcopy_dir} directory", error_messages)
        origcopies = -1

    if transformcopies % 3 != 0:
        message = f"Invalid number of transformcopy files: {transformcopies} generated"
        append_error(message, f"{transformcopy_dir} directory", error_messages)
        transformcopies = -1

    return origcopies, transformcopies


def replace_and_save_text_file(input_file_path, output_file_path):
    # Read the content of the file
    with open(input_file_path, "r", encoding="utf-8") as file:
        content = file.read()

    # Replace occurrences of "_origcopy.csv" with "_transformcopy.csv"
    modified_content = content.replace("_origcopy.csv", "_transformcopy.csv")

    # Save the modified content to a new file
    with open(output_file_path, "w", encoding="utf-8") as file:
        file.write(modified_content)


def collect_primary_keys(data_path):
    primary_keys = []

    directories = glob.glob(os.path.join(data_path, "rad_*_*-*"))
    for directory in directories:
        path = pathlib.PurePath(directory)
        work_dir = os.path.join(directory, "work")

        keys = set()
        for data_file in glob.glob(os.path.join(work_dir, "rad_*_*-*_*_DATA.csv")):
            try:
                data = pd.read_csv(
                    data_file,
                    dtype=str,
                    encoding="utf8",
                    keep_default_na=False,
                    skip_blank_lines=False,
                    nrows=0,
                )
                keys.add(list(data.columns)[0])
            except Exception:
                continue

        primary_keys.append({"directory": path, "keys": sorted(keys)})

    key_df = pd.DataFrame(primary_keys)
    key_df.to_csv(os.path.join(data_path, "phase2_primary_keys.csv"), index=False)


def collect_units(data_path):
    units = []

    directories = glob.glob(os.path.join(data_path, "rad_*_*-*"))
    for directory in directories:
        path = pathlib.PurePath(directory)
        work_dir = os.path.join(directory, "work")

        unit = set()
        for dict_file in glob.glob(os.path.join(work_dir, "rad_*_*-*_*_DICT.csv")):
            try:
                dictionary = pd.read_csv(
                    dict_file,
                    encoding="utf8",
                    dtype=str,
                    keep_default_na=False,
                    skip_blank_lines=False,
                )
                unit.update(list(dictionary["Unit"]))
            except Exception:
                continue

        unit.discard("")
        units.append({"directory": path, "units": sorted(unit)})

    unit_df = pd.DataFrame(units)
    unit_df.to_csv(os.path.join(data_path, "phase2_units.csv"), index=False)
