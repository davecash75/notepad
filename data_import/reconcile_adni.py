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
mr_keep_cols = [
            'image_id','subject_id','study_id','mri_visit',
            'mri_date','mri_description','mri_thickness',
            'mri_mfr','mri_mfr_model','mri_field_str'
            ]

pet_keep_cols = [
            'image_id','subject_id','study_id','pet_visit',
            'pet_date','pet_description','pet_mfr',
            'pet_mfr_model','pet_radiopharm'
            ]

def read_info_sheet(csv_path,modality,conducted_var=None):
    img_info_path = Path(csv_path)
    df_img_info = pd.read_csv(img_info_path)
    df_img_info["session_label"] = df_img_info["subject_id"] + "-" + df_img_info["visit"] + "-" + modality
    # Before going crazy with this refactor make sure we understand
    # WHat is being stored here.
    if conducted_var is not None:
        print("Before filtering images")
        print(df_img_info.shape)
        df_img_info = df_img_info.dropna(subset=conducted_var)
        print("After filtering images")
        print(df_img_info.shape)
    return(df_img_info)

def read_image_sheet(img_study,modality):
    # Load in the MRI data - it's a lot of lot of data
    # So first we are only going to keep a handful of columns
    df_image = pd.read_csv(img_study,low_memory = False)
    if (modality=='MR'):
        df_image = df_image[mr_keep_cols]
        df_image = df_image.rename(
            columns={'mri_visit': 'image_visit',
                    'mri_date': 'image_date',
                    'mri_description': 'image_description'}
            )        
        # Keeping only 3T data (some rando scans with field strength 2.89)
        # And all of the MPRAGE have slice thicknesses less than 1.3
        df_image = df_image.loc[df_image["mri_field_str"]>2.5]
        #df_image = df_image.loc[df_image["mri_thickness"]<1.3]

    else:
        df_image = df_image[pet_keep_cols]
        df_image = df_image.rename(
            columns={'pet_visit': 'image_visit',
                    'pet_date': 'image_date',
                    'pet_description': 'image_description'}
            )
        # Remove FDG and PIB (for time being)
        df_image = df_image.loc[df_image["pet_radiopharm"]!="18F-FDG"]
        df_image = df_image.loc[df_image["pet_radiopharm"]!="11C-PIB"]
    df_image = df_image.sort_values(by=['subject_id','image_date'])
    return df_image

def main():

    parser = argparse.ArgumentParser(
        description='Reconcile ADNI data with NOTEPAD XNAT')
    parser.add_argument('--mr_info', type=str,
                        required=True,
                        help='Location of spreadsheet with MR info')
    parser.add_argument('--pet_info', type=str,
                        required=True,
                        help='Location of spreadsheet with PET info')
    parser.add_argument('--mr_image', type=str,
                        required=True,
                        help='Location of detailed MRI image spreadsheet')
    parser.add_argument('--pet_image', type=str,
                        required=True,
                        help='Location of detailed PET image spreadsheet')
    parser.add_argument('--registry', type=str,
                        required=True,
                        help='Location of registry spreadsheet to reconcile viscodes')

    
    args = parser.parse_args()
    
    # Read in relevant spreadsheet
    # This has readable viscodes
    mr_info_path = Path(args.mr_info)
    df_mr_info = read_info_sheet(mr_info_path,
                                 modality="MR",
                                 conducted_var="MMCONDCT")

    pet_info_path = Path(args.pet_info)
    df_pet_info = read_info_sheet(pet_info_path,
                                  modality="PT",
                                  conducted_var="PET_acquired")

 
    # This has the less readable VISCODE
    # As part of this reconcile, we are going to move
    # images from this VISCODE to the other one.
    mr_image_path = Path(args.mr_image)
    df_mr_image = read_image_sheet(mr_image_path,modality="MR")
    df_mr_image = df_mr_image.groupby(["study_id","subject_id"]).first()

    pet_image_path = Path(args.pet_image)
    df_pet_image = read_image_sheet(pet_image_path,modality="PT")
    df_pet_image = df_pet_image.groupby(["study_id","subject_id"]).first()
    
    registry_path = Path(args.registry)
    df_registry = pd.read_csv(registry_path)
    df_registry = df_registry[
        ["PTID","RID","VISCODE","VISCODE2","EXAMDATE"]]
    df_registry = df_registry.rename(
        columns={'PTID':'subject_id',
                 'VISCODE':'image_visit',
                 'VISCODE2':'visit'})
    df_mr_study = pd.merge(df_mr_image,df_registry,
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


    df_pet_study = pd.merge(df_pet_image,df_registry,
                    on=['subject_id','image_visit'],
                    how='left')
    df_pet_info = pd.merge(df_pet_info, df_pet_study,
                          on=['subject_id','visit'],
                          how='left')
    print(df_pet_info['session_label'])
    radiopharm = df_pet_info['pet_radiopharm'].replace('18F','')
    df_pet_info["session_label"] = df_pet_info["session_label"] + radiopharm
    df_pet_info["alt_session"] = df_pet_info["subject_id"] + "-" + df_pet_info["image_visit"] + "-PET" + radiopharm
    df_pet_info = df_pet_info.set_index("session_label")
    df_pet_info = df_pet_info.sort_index()
    print(df_pet_info)

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
            for mr_session,mr_data in df_mr_info.iterrows():
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
            for pet_session,pet_data in df_pet_info.iterrows():
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
