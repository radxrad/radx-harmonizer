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


def summary(include_dirs, exclude_dirs):
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
    
    data.to_csv(os.path.join(SUMMARY_DIR, "data_element.csv"), index=False)

                 
def get_basic_data(origcopy_dir, summary_dir):
    df_list =[]

    for dict_file in glob.glob(os.path.join(origcopy_dir, f"rad_*_*-*_*_DICT_origcopy.csv")):
        # Collect data elements
        df = utils.get_data_elements(dict_file)

        # Collect metadata
        meta_file = dict_file.replace("_DICT_origcopy.csv", "_META_origcopy.json")
        subproject, phs_identifier, project_num = utils.extract_fields_from_metadata(meta_file)
        df["subproject"] = subproject
        df["phs_id"] = phs_identifier
        df["project_num"] = project_num
        df["radx_id"] = utils.extract_radx_id(meta_file)
        
        df_list.append(df)

    data = pd.concat(df_list)
    data.drop_duplicates(inplace=True)
    return data


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

    # Run the summarizer
    summary(include, exclude)


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
