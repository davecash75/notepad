import sys
from pathlib import Path
import pandas as pd
import xnat 
import re
import time
import shutil
import argparse


# Some helpful globals
# Host for the xnat where data is going
xnat_host = "https://xnat-srv.drc.ion.ucl.ac.uk"
# Project for data
notepad_project = "NOTEPAD_ADNI"

def main():

    parser = argparse.ArgumentParser(
        description='Reconcile ADNI data with NOTEPAD XNAT')
    parser.add_argument('--mr_info', type=str,
                        required=True,
                        help='Location of spreadsheet with MR image info')
    parser.add_argument('--pet_info', type=str,
                        required=True,
                        help='Location of spreadsheet with PET image info')
    args = parser.parse_args()
    
    # Read in relevant spreadsheet
    mr_info_path = Path(args.mr_info)
    df_mr_info = pd.read_csv(mr_info_path)
    df_mr_info["session_label"] = df_mr_info["subject_id"] + "-" + df_mr_info["visit"] + "-MR"
    df_mr_info = df_mr_info.set_index("session_label")
    df_mr_info = df_mr_info.sort_index()
    print("Before filtering MR")
    print(df_mr_info.shape)
    df_mr_info = df_mr_info.dropna(subset="MMCONDCT")
    print("After filtering MR")
    print(df_mr_info.shape)
    
    # Get all MR data and all PET data from XNAT
    # Find which datasets are missing from XNAT and report
    # This may be something that can be across projects
    num_mr_missing = 0
    with xnat.connect(xnat_host) as xnat_session:
        # Get list of subjects for the project. 
        xnat_project = xnat_session.projects[notepad_project]
        xnat_mr_list = xnat_project.experiments.filter({"xsiType":"xnat:mrSessionData"})
        if xnat_mr_list is not None:
            for mr_session,mr_data in df_mr_info.iterrows():
                print(mr_session)
                if mr_session in xnat_mr_list: 
                    print("Match")
                else:
                    num_mr_missing = num_mr_missing + 1                                                  
    print(num_mr_missing)
    
if __name__ == "__main__":
    main()
