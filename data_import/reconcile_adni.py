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
mri_keep_cols = [
            'image_id','subject_id','study_id','mri_visit',
            'mri_date','mri_description','mri_thickness',
            'mri_mfr','mri_mfr_model','mri_field_str'
            ]

pet_keep_cols = [
            'image_id','subject_id','study_id','pet_visit',
            'pet_date','pet_description','pet_mfr',
            'pet_mfr_model','pet_radiopharm'
            ]

def read_info_sheet(csv_path,modality):
    img_info_path = Path(csv_path)
    df_img_info = pd.read_csv(img_info_path)
    if modality == "MR":
        df_img_info  = df_img_info[mri_keep_cols]
        df_img_info = df_img_info.rename(
            columns={'mri_visit': 'image_visit',
                    'mri_date': 'image_date',
                    'mri_description': 'image_description'}
            )
    else:
        df_img_info = df_img_info[pet_keep_cols]
        df_img_info = df_img_info.rename(
            columns={'pet_visit': 'image_visit',
                    'pet_date': 'image_date',
                    'pet_description': 'image_description'}
            )
        # Remove FDG and PIB (for time being)
        df_img_info = df_img_info.loc[df_img_info["pet_radiopharm"]!="11C-PIB"]
   
    return(df_img_info)

def main():

    parser = argparse.ArgumentParser(
        description='Reconcile ADNI data with NOTEPAD XNAT')
    parser.add_argument('--mri', type=str,
                        required=True,
                        help='Location of spreadsheet with MR info')
    parser.add_argument('--pet', type=str,
                        required=True,
                        help='Location of spreadsheet with PET info')
    parser.add_argument('--demog', type=str,
                        required=True,
                        help='Location of detailed MRI image spreadsheet')
    parser.add_argument('--registry', type=str,
                        required=True,
                        help='Location of registry spreadsheet to reconcile viscodes')

    
    args = parser.parse_args()
    
    # Read in relevant spreadsheet
    # This has readable viscodes
    mri_info_path = Path(args.mri)
    df_mri = read_info_sheet(mri_info_path,
                             modality="MR")

    pet_info_path = Path(args.pet)
    df_pet = read_info_sheet(pet_info_path,
                             modality="PT")

    demog_path = Path(args.demog)
    df_demog = pd.read_csv(demog_path)

    registry_path = Path(args.registry)
    df_registry = pd.read_csv(registry_path)
    df_registry = df_registry[
        ["PTID","RID","VISCODE","VISCODE2","EXAMDATE"]
        ]
    df_registry = df_registry.rename(
        columns={'PTID':'subject_id',
                 'VISCODE':'image_visit',
                 'VISCODE2':'visit'})
    
    df_demog = pd.merge(df_demog,df_registry,
                              on=['subject_id','visit'],
                              how='left')

    df_mri = pd.merge(df_mri, df_demog,
                      on=['subject_id','image_visit'],
                      how='left')
    print(df_mri[df_mri['visit'].isna()])
    df_mri['visit'] = df_mri['visit'].fillna("Unknown")
    df_mri["session_label"] = df_mri["subject_id"] + "-" + df_mri["visit"] + "-MR"
    df_mri["alt_session"] = df_mri["subject_id"] + "-" + df_mri["image_visit"] + "-MR"
    df_mri = df_mri.set_index("session_label")
    df_mri = df_mri.sort_index()
    print(df_mri)

    df_pet = pd.merge(df_pet, df_demog,
                      on=['subject_id','image_visit'],
                      how='left')
    radiopharm = df_pet['pet_radiopharm'].str.replace('18F-','')
    print(df_pet[df_pet['visit'].isna()])
    df_pet['visit'] = df_pet['visit'].fillna("Unknown")
    df_pet["session_label"] = df_pet["subject_id"] + "-" + df_pet["visit"] + "-PET-" + radiopharm
    df_pet["alt_session"] = df_pet["subject_id"] + "-" + df_pet["image_visit"] + "-PET-" + radiopharm
    df_pet = df_pet.set_index("session_label")
    df_pet = df_pet.sort_index()
    print(df_pet)
    print("unknown visits")
    print(df_pet.loc[df_pet["visit"]=="Unknown"])
    # Get all MR data and all PET data from XNAT
    # Find which datasets are missing from XNAT and report
    # This may be something that can be across projects
    num_mr_missing = 0
    num_mr_need_changing = 0
    num_pet_missing = 0
    num_pet_need_changing = 0
    with xnat.connect(xnat_host) as xnat_session:
        # Get list of subjects for the project. 
        xnat_project = xnat_session.projects[notepad_project]
        xnat_mr_list = xnat_project.experiments.filter({"xsiType":"xnat:mrSessionData"})
        if xnat_mr_list is not None:
            for mr_session,mr_data in df_mri.iterrows():
                print(mr_session)
                if mr_session in xnat_mr_list: 
                    print("Match")
                # see if it could be in the other visit code
                elif mr_data["alt_session"] in xnat_mr_list:
                    print("Alt match")
                    num_mr_need_changing = num_mr_need_changing + 1
                    # and then change the name
                    print(f"Changing {mr_data['alt_session']} to {mr_session}")
                    xnat_mr_list[mr_data["alt_session"]].label = mr_session
                else:
                    num_mr_missing = num_mr_missing + 1                                                  

        xnat_pet_list = xnat_project.experiments.filter({"xsiType":"xnat:petSessionData"})
        if xnat_pet_list is not None:
            for pet_session,pet_data in df_pet.iterrows():
                print(pet_session)
                if pet_session in xnat_pet_list: 
                    print("Match")
                # see if it could be in the other visit code
                elif pet_data["alt_session"] in xnat_pet_list:
                    print("Alt match")
                    num_pet_need_changing = num_pet_need_changing + 1
                    # and then change the name
                    print(f"Changing {pet_data['alt_session']} to {pet_session}")
                    xnat_pet_list[pet_data["alt_session"]].label = pet_session
                else:
                    num_pet_missing = num_pet_missing + 1  
    print("MRI images:")
    print(f"Number MRI missing: {num_mr_missing}")
    print(f"Number MRI needing changes: {num_mr_need_changing}")
    print("PET images:")
    print(f"Number PET missing: {num_pet_missing}")
    print(f"Number PET needing changes: {num_pet_need_changing}")
    
if __name__ == "__main__":
    main()
