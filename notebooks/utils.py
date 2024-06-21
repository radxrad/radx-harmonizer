#!/usr/bin/python3
import os
import glob
import pathlib
import pandas as pd
import csv

required_fields = {"Variable / Field Name", "Field Label", "Section Header", "Field Type", "Unit", "Choices, Calculations, OR Slider Labels", "Field Note", "CDE Reference"}


def file_is_missing(directory, error_messages):
    all_files = set(glob.glob(os.path.join(directory, "*")))
    data_files = set(glob.glob(os.path.join(directory, "rad_*_*-*_DATA_preorigcopy.csv")))
    dict_files = set(glob.glob(os.path.join(directory, "rad_*_*-*_DICT_preorigcopy.csv")))
    meta_files = set(glob.glob(os.path.join(directory, "rad_*_*-*_META_preorigcopy.csv")))

    # TODO: check if directory and file names rad_XXXX_YYYY-ZZZZ match!
    
    error = False
    # Check for files that don't match the file naming convention
    extra_files = all_files - data_files - dict_files - meta_files
    for extra_file in extra_files:
        message = "Unrecognized file name"
        error_messages.append({"severity": "ERROR", "filename": os.path.basename(extra_file), "message": message})
        error = True

    # Check that the number of DATA, DICT, and META files is the same
    if len(data_files) != len(dict_files) or len(data_files) != len(meta_files):
        message = "DATA, DICT, META file mismatch"
        error_messages.append({"severity": "ERROR", "filename": directory, "message": message})
        error = True
        
    for data_file in data_files:
        # Check for missing DICT files
        dict_file = data_file.replace("_DATA_preorigcopy.csv", "_DICT_preorigcopy.csv")
        if not dict_file in dict_files:
            message = "DICT file missing"
            error_messages.append({"severity": "ERROR", "filename": os.path.basename(dict_file), "message": message})
            error = True

        # Check for missing META files
        meta_file = data_file.replace("_DATA_preorigcopy.csv", "_META_preorigcopy.csv")
        if not meta_file in meta_files:
            message = "META file missing"
            error_messages.append({"severity": "ERROR", "filename": os.path.basename(meta_file), "message": message})
            error = True

    return error, error_messages


def check_meta_file(filename, error_messages):
    data = pd.read_csv(filename, encoding="utf8", dtype=str, keep_default_na=False, skip_blank_lines=False)
    columns = data.columns

    error = False

    if len(columns) != 3:
        message = f"Metadata file has {len(columns)} columns, 3 columns are required"
        error_messages.append({"severity": "ERROR", "filename": os.path.basename(filename), "message": message})
        error = True
        
    if error:
        return error, error_messages

    # check column names
    if columns[0] != "Field Label":
        message = "Field Label column missing"
        error_messages.append({"severity": "ERROR", "filename": os.path.basename(filename), "message": message})
        error = True
    if columns[1] != "Choices":
        message = "Choices column missing"
        error_messages.append({"severity": "ERROR", "filename": os.path.basename(filename), "message": message})
        error = True
    if columns[2] != "Description":
        message = "Description column missing"
        error_messages.append({"severity": "ERROR", "filename": os.path.basename(filename), "message": message})
        error = True
        
    if error:
        return error, error_messages

    # check the number of data files
    filenames = data[data["Field Label"] == "number_of_datafiles_in_this_package"]
    if filenames.shape[0] != 1:
        message = "Row 'number_of_datafiles_in_this_package' is missing"
        error_messages.append({"severity": "ERROR", "filename": os.path.basename(filename), "message": message})
        error = True

    if error:
        return error, error_messages

    num_files = filenames["Choices"].tolist()
    if num_files[0] != "1":
        message = f"number_of_datafiles_in_this_package is {num_files[0]}, it must be 1"
        error_messages.append({"severity": "ERROR", "filename": os.path.basename(filename), "message": message})
        error = True

    # check data file name
    filenames = data[data["Field Label"] == "datafile_names - add_additional_rows_as_needed"]
    if filenames.shape[0] != 1:
        message = "Row 'datafile_names - add_additional_rows_as_needed' is missing"
        error_messages.append({"severity": "ERROR", "filename": os.path.basename(filename), "message": message})
        error = True

    if error:
        return error, error_messages

    data_file = os.path.basename(filename).replace("_META_", "_DATA_")
    data_files = filenames["Choices"].tolist()
    if data_files[0] != data_file:
        message = f"Data file name: {data_files[0]} doesn't match"
        error_messages.append({"severity": "ERROR", "filename": os.path.basename(filename), "message": message})
        error = True

    description = filenames["Description"].tolist()
    description = description[0]
    if description == "":
        message = "Data file description is missing"
        error_messages.append({"severity": "WARN", "filename": os.path.basename(filename), "message": message})
        error = True
  
    return error, error_messages


def is_not_utf8_encoded(filename, error_messages):
    import traceback

    error = False
    try:
        data = pd.read_csv(filename, encoding="utf8", skip_blank_lines=False)
    except Exception:
        message = f"Not utf-8 encoded or invalid csv file: {traceback.format_exc().splitlines()[-1]}"
        error_messages.append({"severity": "ERROR", "filename": os.path.basename(filename), "message": message})
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
        error_messages.append({"severity": "ERROR", "filename": os.path.basename(filename), "message": message})
        error = True

    return error, error_messages


def convert_iso_to_utf8(orig_filename, fixed_filename, error_messages):
    try:
        data = pd.read_csv(orig_filename, encoding="ISO-8859-1", dtype=str, keep_default_na=False, skip_blank_lines=False)
        data.to_csv(fixed_filename, encoding="utf-8", index=False)
        message = "file was automatically converted to utf-8"
        error_messages.append({"severity": "WARN", "filename": os.path.basename(fixed_file), "message": message})
    except Exception:
        message = traceback.format_exc().splitlines()[-1]
        error_messages.append({"severity": "ERROR", "filename": os.path.basename(filename), "message": message})
        error = True
        return error, error_messages
        
    return is_not_utf8_encoded(fixed_filename, error_messages)


def check_column_names(data, error_messages):
    error = False
    if len(data.columns) != data.shape[1]:
        message = "header does not match data"
        error_messages.append({"severity": "ERROR", "filename": os.path.basename(filename), "message": message})
        error = True
    for col in data.columns:
        col_stripped = col.strip()
        if col_stripped != col:
            message = f"column header: '{col}' contains spaces"
            error_messages.append({"severity": "ERROR", "filename": os.path.basename(filename), "message": message})
            error = True
        if col_stripped == "":
            message = "Empty column name"
            error_messages.append({"severity": "ERROR", "filename": os.path.basename(filename), "message": message})
            error = True
        if "Unnamed" in col:
            message = f"Unnamed column: {col}"
            error_messages.append({"severity": "ERROR", "filename": os.path.basename(filename), "message": message})
            error = True
            
    return error, error_messages

    
def remove_empty_rows_cols(input_file, output_file, error_messages):
    data = pd.read_csv(input_file, encoding="utf8", dtype=str, keep_default_na=False, skip_blank_lines=False)
    # TODO remove whitespace from the header
    
    # remove leading and trailing whitespace
    data = data.map(lambda x: x.strip())
    # identify rows with all empty strings
    empty_row_mask = data.eq('').all(axis=1)
    data = data[~empty_row_mask]
    # identify columns with all empty strings
    empty_col_mask = data.eq('').all(axis=0)
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
        error_messages.append({"severity": "ERROR", "filename": os.path.basename(filename), "message": message})   

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


def has_non_printable_chars(filename):
    """
    Checks if a CSV file contains any non-printable characters.

    Args:
      filename: The path to the CSV file.

    Returns:
      True if the file contains non-printable characters, False otherwise.
    """
    with open(filename, 'r', newline='') as csvfile:
       reader = csv.reader(csvfile)
       non_printable = False
       rows = 0

       for row in reader:
          col = 0
          for cell in row:
              pos = 0
              for char in cell:
                  if not char.isprintable():
                      print(f"{rows}-{col} non-printable char: {char}")
                      non_printable = True
                  pos = pos + 1
              col = col + 1
          rows = rows + 1

             #if not all(char.isprintable() for char in cell):
             #   print(cell)
             #   return True
    return non_printable


def save_error_file(error_messages, error_file):
    df = pd.DataFrame(error_messages)
    if len(df) > 0:
        df.to_csv(os.path.join(error_file), index=False) 


def download_data_element_templates():
    import pandas as pd

    # Parse minimum common data elements for RADx-rad projects
    min_cdes = pd.read_csv(
        "https://docs.google.com/spreadsheets/d/1LaGrgk1N8B2EclU1w2bduqHJ6p1pN4Mq/export?format=csv",
        keep_default_na=False,
    )
    min_cdes["template"] = "Minimum CDEs"
    #print("Minimum CDEs", min_cdes.shape)

    # Parse technology description data elements
    technology_description = pd.read_csv(
        "https://docs.google.com/spreadsheets/d/1DETG54TF83vPhvrW-MLN5p9QJiMlVA3j/export?format=csv",
        keep_default_na=False,
    )
    technology_description["template"] = "Technology Description"
    #print("Technology Description", technology_description.shape)

    # Parse data elements for PCR data
    pcr = pd.read_csv(
        "https://docs.google.com/spreadsheets/d/1iJo9uu3FcvBngxrSM0JCqmXDNghMMxcr/export?format=csv",
        keep_default_na=False,
    )
    pcr["template"] = "PCR"
    #print("PCR", pcr.shape)

    # Parse data elements for spiked samples
    spiked_samples = pd.read_csv(
        "https://docs.google.com/spreadsheets/d/13eew7mJOp0fbh_hs8SbUGHtnZvZ7giqp/export?format=csv",
        keep_default_na=False,
    )
    spiked_samples["template"] = "Spiked Samples"
    #print("Spiked Samples", spiked_samples.shape)

    # Parse data elements for clinical samples
    clinical_samples = pd.read_csv(
        "https://docs.google.com/spreadsheets/d/1uNN9MaEjgkhHX4rY-HujbzltlilY_NCN/export?format=csv",
        keep_default_na=False,
    )
    clinical_samples["template"] = "Clinical Samples"
    #print("Clinical Samples", clinical_samples.shape)

    # Parse data elements for wastewater projects
    waste_water = pd.read_csv(
        "https://docs.google.com/spreadsheets/d/1Si4YHCZ0Hh2EFM--VMFaDWi0SAcGl96c-qes82nV2tA/export?format=csv",
        keep_default_na=False,
    )
    waste_water["template"] = "Waste Water"
    #print("Waste Water", waste_water.shape)

    # Parse data elements for test results
    test_results = pd.read_csv(
        "https://docs.google.com/spreadsheets/d/1m8dkOrerxwaT8xOUWwR5ZE0VPeVvsu04/export?format=csv",
        keep_default_na=False,
    )
    test_results["template"] = "Test Results"
    #print("Test Results", test_results.shape)

    # Parse data elements for performance metrics
    # https://docs.google.com/spreadsheets/d/1yJG7Vt0AmXQkxForxsezgT-9EDRZJwnH/edit?usp=sharing&ouid=112820493940716707493&rtpof=true&sd=true
    performance_metrics = pd.read_csv(
        "https://docs.google.com/spreadsheets/d/1yJG7Vt0AmXQkxForxsezgT-9EDRZJwnH/export?format=csv",
        keep_default_na=False,
    )
    
    # performance_metrics = pd.read_csv("../RADx-rad_Performance_Metrics_Data_Element_Template_v000.xlsx - RADxrad_Data_Element_Template_v.csv")
    performance_metrics["template"] = "Performance Metrics"
    #print("Performance Metrics", performance_metrics.shape)

    # Concatenate all data elements into a single dataframe
    data_elements = pd.concat(
        [
            min_cdes,
            technology_description,
            pcr,
            spiked_samples,
            clinical_samples,
            waste_water,
            test_results,
            performance_metrics,
        ]
    )

    return data_elements


def match_minimum_cdes(data_elements, column_names):
    min_cdes = data_elements[data_elements["template"] == "Minimum CDEs"].copy()
    min_cdes.drop(columns=["template"], inplace=True)
    min_cde_matches = min_cdes.merge(column_names, on="Variable / Field Name")
    min_cde_matches.drop_duplicates(subset=["Variable / Field Name"], inplace=True)

    return min_cde_matches


def add_field_label(field_name):
    if field_name.endswith("_unit"):
        field_label = "Unit of " + field_name.split("_unit")[0]
    else:
        field_label = ""

    return field_label


def get_undefined_data_elements(data_elements, column_names):
    undefined = column_names.merge(
        data_elements, on="Variable / Field Name", how="left", indicator=True
    )
    undefined = undefined.query("_merge == 'left_only'")
    undefined = undefined[["Variable / Field Name", "File Name"]]
    # undefined["Variable / Field Name"] = undefined["Variable / Field Name"].str.strip()
    undefined.drop_duplicates(
        subset="Variable / Field Name", keep="first", inplace=True
    )
    undefined["Field Label"] = undefined["Variable / Field Name"].apply(add_field_label)

    return undefined


def check_empty_field(field_name, data_elements):
    fields = set(data_elements.columns)
    if field_name in fields:
        empty_fields = data_elements[data_elements[field_name] == ""]
        if empty_fields.shape[0] == 0:
            return False
        else:
            print(f"ERROR: Data missing in field: {field_name}")
            return True
    else:
        print(f"ERROR: Data field missing: {field_name}")
        return True


def check_whitespace(field_name, data_elements):
    whitespace = data_elements[data_elements[field_name].str.contains(" ")]
    if whitespace.shape[0] == 0:
        print(f"{field_name}: ok")
        return

    print(f"Data elements with whitespace in {field_name}:")
    return whitespace


def check_field_types(data_elements):
    allowed_types = {
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
    field_types = set(data_elements["Field Type"].unique())
    invalid_field_types = field_types - allowed_types

    if len(invalid_field_types) > 0:
        print(f"ERROR: Invalid Field Type: {invalid_field_types}")
        print(f"INFO : Allowed Field Types: {allowed_types}")
        return list(invalid_field_types)
        
    return ""


def check_required_fields(data_elements):
    fields = set(data_elements.columns)
    missing_fields = required_fields - fields
    if len(missing_fields) > 0:
        print(f"ERROR: Data field missing: {missing_fields}")
        print(f"INFO : Extra fields: {fields - required_fields}")
        return missing_fields
 
    return ""
        

def download_excel_and_parse_column_names(excel_file_name):
    import pandas as pd
    import os
    
    directory, file_path = os.path.split(excel_file_name)
    file_name, extension = os.path.splitext(file_path)

    # download all sheets into a dictionary
    data_sheets = pd.read_excel(excel_file_name, sheet_name=None)

    # extract column and sheet names
    records = []
    for sheet in data_sheets.items():
        columns = sheet[1].columns
        sheet_name = sheet[0]
        print(f"{excel_file_name} - {sheet_name}")
        for column in columns:
            records.append([column, sheet_name])

    # create a dataframe with the column and sheet names
    column_names = pd.DataFrame(records)
    column_names.columns = ["Variable / Field Name", "File Name"]

    return column_names, directory, file_name


def download_google_sheet_excel_and_parse_column_names(google_doc_id, data_dir):
    import pandas as pd
    
    # download all sheets into a dictionary
    data_sheets =  pd.read_excel(f"https://docs.google.com/spreadsheets/d/{google_doc_id}/export?format=xlsx", sheet_name=None)

    # extract column and sheet names
    records = []
    for sheet in data_sheets.items():
        columns = sheet[1].columns
        sheet_name = sheet[0]
        print(f"{google_doc_id} - {sheet_name}")
        for column in columns:
            records.append([column, sheet_name])

    # create a dataframe with the column and sheet names
    column_names = pd.DataFrame(records)
    column_names.columns = ["Variable / Field Name", "File Name"]

    return column_names, data_dir, google_doc_id


def download_excel_dir_and_parse_column_names(excel_dir_path_name):
    import os
    import glob
    import pandas as pd
    
    directory = excel_dir_path_name
    file_name = directory.rsplit("/", 1)[1]
    
    # extract column names from all excel files
    records = []
    # accept both .xls and .xlsx extensions
    for path in glob.glob(os.path.join(excel_dir_path_name, "*xls*")):
        print(path)
        columns = list(pd.read_excel(path).columns)
        print(os.path.split(path)[1], columns)
        
        for column in columns:
            records.append([column, os.path.split(path)[1]])

    # create a dataframe with the column and sheet names
    column_names = pd.DataFrame(records)
    column_names.columns = ["Variable / Field Name", "File Name"]
    
    return column_names, directory, file_name


def download_csv_dir_and_parse_column_names(csv_dir_path_name):
    import os
    import glob
    import pandas as pd
    
    directory = csv_dir_path_name
    file_name = directory.rsplit("/", 1)[1]
    
    # extract column names from all csv files
    records = []
    for path in glob.glob(os.path.join(csv_dir_path_name, "*.csv")):
        # ignore existing output files
        if path.endswith("_DICT.csv") or path.endswith("_UNDEFINED.csv") or path.endswith("_UNITS.csv"):
            continue
            
        print(path)
        columns = list(pd.read_csv(path).columns)
        print(os.path.split(path)[1], columns)
        
        for column in columns:
            records.append([column, os.path.split(path)[1]])

    # create a dataframe with the column and sheet names
    column_names = pd.DataFrame(records)
    column_names.columns = ["Variable / Field Name", "File Name"]
    
    return column_names, directory, file_name
    