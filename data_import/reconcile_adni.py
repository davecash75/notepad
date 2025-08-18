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
    parser.add_argument('--mr_study', type=str,
                        required=True,
                        help='Location of detailed MRI image spreadsheet')
    parser.add_argument('--pet_study', type=str,
                        required=True,
                        help='Location of detailed PET image spreadsheet')
    parser.add_argument('--registry', type=str,
                        required=True,
                        help='Location of registry spreadsheet to reconcile viscodes')

    
    args = parser.parse_args()
    
    # Read in relevant spreadsheet
    # This has readable viscodes
    mr_info_path = Path(args.mr_info)
    df_mr_info = pd.read_csv(mr_info_path)
    df_mr_info["session_label"] = df_mr_info["subject_id"] + "-" + df_mr_info["visit"] + "-MR"
    print("Before filtering MR")
    print(df_mr_info.shape)
    df_mr_info = df_mr_info.dropna(subset="MMCONDCT")
    print("After filtering MR")
    print(df_mr_info.shape)
 
    # This has the less readable VISCODE
    # As part of this reconcile, we are going to move
    # images from this VISCODE to the other one.
    mr_study_path = Path(args.mr_study)
    df_mr_study = pd.read_csv(mr_study_path,low_memory = False)
    df_mr_study = df_mr_study[
        ["subject_id","study_id","mri_visit","mri_date","mri_field_str"]
    ]
    df_mr_study = df_mr_study.loc[df_mr_study["mri_field_str"]>2.5]
    df_mr_study = df_mr_study.rename(columns={'mri_visit':'image_visit'})
    df_mr_study = df_mr_study.groupby(["study_id","subject_id"]).first()
    registry_path = Path(args.registry)
    df_registry = pd.read_csv(registry_path)
    df_registry = df_registry[
        ["PTID","RID","VISCODE","VISCODE2","EXAMDATE"]]
    df_registry = df_registry.rename(
        columns={'PTID':'subject_id',
                 'VISCODE':'image_visit',
                 'VISCODE2':'visit'})
    df_mr_study = pd.merge(df_mr_study,df_registry,
                    on=['subject_id','image_visit'],
                    how='left')
    df_mr_info = pd.merge(df_mr_info, df_mr_study,
                          on=['subject_id','visit'],
                          how='left')
    print(df_mr_info['session_label'])
    df_mr_info = df_mr_info.set_index("session_label")
    df_mr_info = df_mr_info.sort_index()

    df_mr_info["alt_session"] = df_mr_info["subject_id"] + "-" + df_mr_info["image_visit"] + "-MR"
    print(df_mr_info)

    # Get all MR data and all PET data from XNAT
    # Find which datasets are missing from XNAT and report
    # This may be something that can be across projects
    num_mr_missing = 0
    num_mr_need_changing = 0
    with xnat.connect(xnat_host) as xnat_session:
        # Get list of subjects for the project. 
        xnat_project = xnat_session.projects[notepad_project]
        xnat_mr_list = xnat_project.experiments.filter({"xsiType":"xnat:mrSessionData"})
        if xnat_mr_list is not None:
            for mr_session,mr_data in df_mr_info.iterrows():
                print(mr_session)
                if mr_session in xnat_mr_list: 
                    print("Match")
                # see if it could be in the other visit code
                elif mr_data["alt_session"] in xnat_mr_list:
                    print("Alt match")
                    num_mr_need_changing = num_mr_need_changing + 1
                    # and then change the name
                else:
                    num_mr_missing = num_mr_missing + 1                                                  
    print(f"Number missing: {num_mr_missing}")
    print(f"Number needing changes: {num_mr_need_changing}")
    
if __name__ == "__main__":
    main()
