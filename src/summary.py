#!/usr/bin/python3

import argparse
import glob
import os
import sys

import pandas as pd
import utils

# Root directory on AWS
# ROOT_DIR = "r:/"

# Root directory local installation
ROOT_DIR = ".."

# File paths
DATA_DIR = os.path.join(ROOT_DIR, "data_harmonized")
SUMMARY_DIR = os.path.join(ROOT_DIR, "summary")
TIER1_HARMONIZED_DICT = os.path.join(ROOT_DIR, "reference/RADx-rad_tier1_dict_2024-10-28.csv")
TIER2_HARMONIZED_DICT = os.path.join(ROOT_DIR, "reference/RADx-rad_tier2_dict_2024-10-28.csv")


def data_element_summary(include_dirs, exclude_dirs):
    directories = utils.get_directories(include_dirs, exclude_dirs, DATA_DIR)

    df_list = []
    for directory in directories:
        origcopy_dir = os.path.join(directory, "origcopy")
        if os.path.exists(origcopy_dir):
            df = get_basic_data(origcopy_dir, SUMMARY_DIR)
            df_list.append(df)
        else:
            raise ValueError(f"Error: {origcopy_dir} does not exist!")

    os.makedirs(SUMMARY_DIR, exist_ok=True)
    
    data = pd.concat(df_list)
    data = utils.assign_data_element_tier(data, TIER1_HARMONIZED_DICT, TIER2_HARMONIZED_DICT)
    data.to_csv(os.path.join(SUMMARY_DIR, "data_elements.csv"), index=False)
    print(f"Data element summary saved to: {os.path.join(SUMMARY_DIR, 'data_elements.csv')}")


def publication_summary(include_dirs, exclude_dirs):
    directories = utils.get_directories(include_dirs, exclude_dirs, DATA_DIR)

    df_list = []
    for directory in directories:
        origcopy_dir = os.path.join(directory, "origcopy")
        if os.path.exists(origcopy_dir):
            df = get_publications(origcopy_dir, SUMMARY_DIR)
            df_list.append(df)
        else:
            raise ValueError(f"Error: {origcopy_dir} does not exist!")

    os.makedirs(SUMMARY_DIR, exist_ok=True)
    
    data = pd.concat(df_list)
    data.to_csv(os.path.join(SUMMARY_DIR, "publications.csv"), index=False)
    print(f"Publication summary saved to: {os.path.join(SUMMARY_DIR, 'publications.csv')}")

                 
def get_basic_data(origcopy_dir, summary_dir):
    df_list =[]

    for dict_file in glob.glob(os.path.join(origcopy_dir, f"rad_*_*-*_*_DICT_origcopy.csv")):
        # Collect data elements
        df = utils.get_data_elements(dict_file)

        # Collect metadata
        meta_file = dict_file.replace("_DICT_origcopy.csv", "_META_origcopy.json")
        subproject, phs_identifier, project_num, _ = utils.extract_fields_from_metadata(meta_file)
        df["subproject"] = subproject
        df["phs_id"] = phs_identifier
        df["project_num"] = project_num
        df["radx_id"] = utils.extract_radx_id(meta_file)
        
        df_list.append(df)

    data = pd.concat(df_list)
    data.drop_duplicates(inplace=True)
    return data


def get_publications(origcopy_dir, summary_dir):
    df_list =[]

    for meta_file in glob.glob(os.path.join(origcopy_dir, f"rad_*_*-*_*_META_origcopy.json")):
        # Collect metadata
        subproject, phs_identifier, project_num, publications = utils.extract_fields_from_metadata(meta_file)
        for publication in publications:
            row = {
                   'subproject': [subproject],
                   'phs_identifier': [phs_identifier],
                   'project_num': [project_num],
                   'publications': [publication],
                   'radx_id': [ utils.extract_radx_id(meta_file)],
            }
            df = pd.DataFrame(row)
            df_list.append(df)

    if len(df_list) > 0:
        data = pd.concat(df_list)
        data.drop_duplicates(inplace=True)
    else:
        data = pd.DataFrame()

    return data


def main(include, exclude):
    print("summary: Create a summary of data elements and publications for RADx-rad datasets.")

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

    # Collect data elements
    data_element_summary(include, exclude)
    
    # Collect publications
    publication_summary(include, exclude)


if __name__ == "__main__":
    # Create the parser
    parser = argparse.ArgumentParser(
        description="Create summary of RADx-rad datasets."
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
