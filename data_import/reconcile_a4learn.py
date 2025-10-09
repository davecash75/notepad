import sys
from pathlib import Path
import pandas as pd
import xnat 
import re
import time
import shutil
import argparse

xnat_host = "https://xnat-srv.drc.ion.ucl.ac.uk"
# Project for data
notepad_project = "NOTEPAD_A4"

def check_experiments(df_img,xnat_project,
                      modality,suppress_match=True):
    num_archived = 0
    num_missing = 0
    num_not_archived = 0
    for row in df_img.itertuples():
        session_label = f"{row.BID}-{row.VISCODE}-{modality}"
        img_status = "Match"
        xnat_subject_list = xnat_project.subjects
        if row.BID not in xnat_subject_list:
            img_status = "Subject not found"
            num_missing = num_missing + 1
            continue
        xnat_subject = xnat_subject_list[row.BID]
        xnat_experiment_list = xnat_subject.experiments
        if session_label not in xnat_experiment_list:
            img_status = "Session not found"
            num_missing = num_missing + 1
            continue
        num_archived = num_archived + 1
        if not suppress_match or img_status != "Match": 
            print(f"{session_label} - {img_status}")
    return (num_archived,num_missing,num_not_archived)

def main():
    parser = argparse.ArgumentParser(
        description='Reconcile A4/LEARN with NOTEPAD XNAT')

    parser.add_argument('--in_path', type=str,
                    required=True,
                    help='Path to data')
    parser.add_argument('--check_path',type=str,
                        help="Location of root directory where downloaded files are to see if there is issue"
                        )
    parser.add_argument('--suppress_match',action='store_true',
                        help="don't print information about hits.")
    
    args = parser.parse_args()
    if args.check_path:
        image_root = Path(args.check_path)

    in_dir = Path(args.in_path)
    mri_path = in_dir / "External Data" / "imaging_volumetric_mri.csv"
    # All we need is Subject ID, Visit Code so that we can check if uploaded
    df_mri = pd.read_csv(mri_path,low_memory=False,
                         dtype = {'VISCODE': 'str'}
                         )
    df_mri = df_mri.loc[:,['SUBSTUDY','BID','VISCODE',
                           'Date_DAYS_CONSENT',
                           'Date_DAYS_T0']]

    amy_path = in_dir / "External Data" / "imaging_SUVR_amyloid.csv"
    df_amy = pd.read_csv(amy_path,low_memory=False,                       
                         dtype = {'VISCODE': 'str'}
                         )
    df_amy = df_amy.loc[:,'SUBSTUDY':'scan_analyzed']
    print(len(df_amy))
    df_amy = df_amy.drop_duplicates()
    print(len(df_amy))
    # All we need is Subject ID, Visit Code so that we can check if uploaded

    tau_path = in_dir / "External Data" / "imaging_SUVR_tau.csv"
    df_tau = pd.read_csv(tau_path,low_memory=False,
                         dtype = {'VISCODE': 'str'}
                         )
    df_tau = df_tau.loc[:,'SUBSTUDY':'scan_analyzed']
    print(len(df_tau))
    df_tau = df_tau.drop_duplicates()
    print(len(df_tau))
    num_mr_missing = 0
    num_mr_not_archived = 0
    num_mr_archived = 0
    num_amy_missing = 0
    num_amy_not_archived = 0
    num_amy_archived = 0
    num_tau_missing = 0
    num_tau_not_archived = 0
    num_tau_archived = 0
    with xnat.connect(xnat_host) as xnat_session:
        xnat_project = xnat_session.projects[notepad_project]
        num_mr_archived,num_mr_missing,num_mr_not_archived = check_experiments(
            df_mri,
            xnat_project,
            'MR',
            args.suppress_match)
        num_amy_archived,num_amy_missing,num_amy_not_archived = check_experiments(
            df_amy,
            xnat_project,
            'PET-FBP',
            args.suppress_match)
        num_tau_archived,num_tau_missing,num_tau_not_archived = check_experiments(
            df_tau,
            xnat_project,
            'PET-AV1451',
            args.suppress_match)


    print("MRI images:")
    print(f"Number MRI archived in XNAT: {num_mr_archived}")
    print(f"Number MRI missing: {num_mr_missing}")

#    if image_root is not None:
#        print(f"Number of MRI present but not archived: {num_mr_not_archived}")

    print("FBP AMY PET images:")
    print(f"Number FBP AMY PET archived in XNAT: {num_amy_archived}")
    print(f"Number FBP AMY PET missing: {num_amy_missing}")
    print("FTP PET images:")
    print(f"Number FTP PET archived in XNAT: {num_tau_archived}")
    print(f"Number FTP PET missing: {num_tau_missing}")

#    if image_root is not None:
#        print(f"Number of PET present but not archived: {num_pet_not_archived}")
               

if __name__ == "__main__":
    main()
