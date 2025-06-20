import sys
import re
import time
from pathlib import Path
from zipfile import ZipFile
import shutil
import argparse
import pandas as pd
import pydicom as dcm
import xnat 
import heudiconv

# Some helpful globals
# Host for the xnat where data is going
xnat_host = "https://xnat-srv.drc.ion.ucl.ac.uk"
# Project for data
notepad_project = "NOTEPAD_ADNI"
# Regex patterns to parse Subject and Image ID from paths
subject_id_pattern = re.compile(r"^\d{3}_S_\d{4,5}$")
image_id_pattern = re.compile(r"^I\d+$")
file_pattern = re.compile(r"^ADNI_(\d{3}_S_\d{4,5})_.*_S(\d+)_I(\d+).[dn].*")
datetime_pattern = re.compile(r"^20\d{2}-[01]\d-[0123]\d_[012]\d_[0-5]\d_[0-5]\d")

# Which columns from ADNI MRI and PET spreadsheets should be kept
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

# Dictionaries to encode integers into correct strings
# For gender, ethnicity, race
gender_map = {
    1 : 'male',
    2 : 'female'
}

ethnicity_map = {
    1: "Hispanic or Latino",
    2: "Not Hispanic or Latino",
    3: "Unknown"
}

race_map = {
    1: "American Indian or Alaskan Native",
    2: "Asian",
    3: "Native Hawaiian or Other Pacific Islander",
    4: "Black or African American",
    5: "White",
    6: "More than one race",
    7: "Unknown"
}

# This just extracts the bit of path that matches the pattern
# and returns it
def extract_from_path(input_path,input_pattern):
    out_key = None 
    # Parse the path to get the subject ID and image ID
    path_parts = input_path.parts
    for p in path_parts:
        hit = re.match(input_pattern,p)
        if hit:
            out_key = hit.group()
            return out_key

def get_image_ids(in_path,path_glob,id_list):
    file_path_list = in_path.glob(path_glob)
    dir_path_list = set([x.parent for x in file_path_list])
    for p in dir_path_list:
        image_id = extract_from_path(p,image_id_pattern)
        image_id = int(image_id.replace('I',''))
        if image_id not in id_list:
            id_list.append(image_id)

def parse_image_filename(file_path):
    file_info = re.match(file_pattern,file_path.name)
    if file_info is None:
        image_id = extract_from_path(file_path.parent,image_id_pattern)
        image_id = int(image_id.replace('I',''))
        scandate = extract_from_path(file_path.parent,datetime_pattern)
        series_id = scandate.replace('_','')
        series_id = int(series_id.replace('-',''))
    else:
        image_id = int(file_info.group(3))
        series_id = int(file_info.group(2))
    return (image_id, series_id)

def process_image_list(subject_id,image_list,adni_studies,
                       df_mr,df_pet,
                       dcm_flag=True):
    current_image_id = None
    df_session = None
    adni_info={}
    for f in image_list:
        image_id, series_id = parse_image_filename(f)
        # To avoid reading in the spreadsheet for every file
        # Just change it when a new image pops up
        if image_id != current_image_id:
            # Grab releant info from image spreadsheets
            modality=""
            xnat_session_label = None
            if image_id in df_mr.index:
                df_session = df_mr.loc[image_id].squeeze()
                modality = "MR"
            elif image_id in df_pet.index:
                df_session = df_pet.loc[image_id].squeeze()
                radiopharm = df_session['pet_radiopharm'].replace('18F-','')
                modality = f"PET-{radiopharm}"
            else:
                print(f'WARNING: Could not find {image_id} in the spreadsheets')
                print('Skipping this session for now')
                continue
            # Make a dict to store the relevant information
            # So it is to hand for the next image
            # if it is from the same image ID
            adni_info['image_id'] = image_id
            adni_info['series_id'] = series_id
            adni_info['visit_id'] = df_session['visit']
            adni_info['study_id'] = int(df_session['study_id'])
            adni_info['image_date'] = df_session['image_date']
            image_description = df_session['image_description']
            image_description = image_description.replace(';','_')
            image_description = image_description.replace(' ','_')
            adni_info['image_description'] = image_description
            adni_info['session_label'] = f"{subject_id}-{adni_info['visit_id']}-{modality}"
            current_image_id = image_id
        # If we don't have information for this study ID
        # Add it
        if adni_info['study_id'] not in adni_studies:
            study_info = {
                'modality': modality,
                'visit_id': adni_info['visit_id'],
                'image_date': adni_info['image_date'],
                'session_id': adni_info['session_label'],
                'series_list' : {},
            }
            adni_studies[adni_info['study_id']] = study_info
        series_map = adni_studies[adni_info['study_id']]['series_list']
        if adni_info['series_id'] not in series_map:
            xnat_scan_number = str(adni_info['series_id'])
            if dcm_flag:
                with dcm.dcmread(f) as ds:
                    if ds.SeriesNumber is not None:
                        xnat_scan_number = str(ds.SeriesNumber)
            series_info = {
                'scan_number': xnat_scan_number,
                'image_list':{},
                }
            print(series_id)
            print(series_info['scan_number'])
            series_map[adni_info['series_id']] = series_info
        image_map = series_map[adni_info['series_id']]['image_list']
        if adni_info['image_id'] not in image_map:
            image_info = {
                'image_description': adni_info['image_description'],
                'dcm_files': [],
                'nii_files': [],
            }
            image_map[adni_info['image_id']] = image_info
        if dcm_flag:        
            image_map[adni_info['image_id']]['dcm_files'].append(f)
        else:
            image_map[adni_info['image_id']]['nii_files'].append(f)
            


# This processes the study sheet of subject metadata
def process_study_sheet(img_info):
    df_info = pd.read_csv(img_info)
    df_info = df_info.sort_values(by=['subject_id','visit'])
    # A bit of cleaning up on the racial category
    # So that it will map properly
    df_info.loc[df_info['PTRACCAT']=='9','PTRACCAT'] = 7
    df_info.loc[df_info['PTRACCAT']=='1|4','PTRACCAT'] = 6
    df_info.loc[df_info['PTRACCAT']=='1|5','PTRACCAT'] = 6
    df_info.loc[df_info['PTRACCAT']=='2|4','PTRACCAT'] = 6
    df_info.loc[df_info['PTRACCAT']=='2|5','PTRACCAT'] = 6
    df_info.loc[df_info['PTRACCAT']=='4|5','PTRACCAT'] = 6
    df_info.loc[df_info['PTRACCAT']=='3|4|5','PTRACCAT'] = 6
    df_info['PTRACCAT'] = pd.to_numeric(df_info['PTRACCAT'])
    df_info['PTETHCAT_STR'] = df_info['PTETHCAT'].map(ethnicity_map)
    df_info['PTRACCAT_STR'] = df_info['PTRACCAT'].map(race_map)
    df_info['PTGENDER_STR'] = df_info['PTGENDER'].map(gender_map)
    return df_info

# This processes the imaging metadata sheet
def process_image_sheet(img_study,modality):
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

def get_scan_number(dcm_file_list):
    scan_number_list = []
    for f in dcm_file_list:
        with dcm.dcmread(f) as ds:
            if (ds.SeriesNumber is not None) and (ds.SeriesNumber not in scan_number_list):
                scan_number_list.append(ds.SeriesNumber)
    return scan_number_list


def make_dcm_zip(dcm_list,study_id):
    zip_path = Path('/tmp',f'{study_id}.zip')
    study_uids = []
    series_uids = []
    make_new_uid = False
    create_series_number=False
    for dcm_up in dcm_list:
        ds = dcm.dcmread(dcm_up)
        if ds.SeriesNumber is None:
            create_series_number=True
        study_uids.append(ds.StudyInstanceUID)
        series_uids.append(ds.SeriesInstanceUID)
    study_uid_set = set(study_uids)
    series_uid_set = set(series_uids)
    if len(study_uid_set) > 1:
        print('Multiple UIDs detected')
        print(study_uid_set)
        make_new_uid=True
    if make_new_uid or create_series_number:
        temp_dcm_dir=Path('/tmp',f'{study_id}-temp')
        temp_dcm_dir.mkdir(exist_ok=True,parents=True)
        new_study_uid = dcm.uid.generate_uid()
        for dcm_up in dcm_list:
            adni_image_id, adni_series_id = parse_image_filename(dcm_up)
            ds = dcm.dcmread(dcm_up)
            if make_new_uid:
                ds.StudyInstanceUID = new_study_uid
            if create_series_number:
                ds.SeriesNumber=adni_series_id
            dcm_out = temp_dcm_dir / dcm_up.name
            ds.save_as(dcm_out)
      
    with ZipFile(zip_path,'w') as import_zip:
        if make_new_uid or create_series_number:
            for dcm_up in temp_dcm_dir.glob('*.dcm'):
                import_zip.write(dcm_up)
        else:
            for dcm_up in dcm_list:
                import_zip.write(dcm_up)

    if make_new_uid or create_series_number:
        shutil.rmtree(temp_dcm_dir)
    return(zip_path)
                

# Refactor: Look at a whole subject's data
# Group the scans by study into one Zip file
# To avoid issues around Autorun and double archive
# Specify the PATH and the subject ID
# Remove .dcm files when done, but keep Nifti for now.



# This program will take in data from 
# DICOM and use DICOM Inbox to import
# to archive the data into XNAT
# How to track whats been uploaded effectively
# The process will be
# 1. Look at a directory (whether command line or spreadsheet)
# 2. Is there a subject for this directory? If not create it using key demographic data
# 3. Is there a session matching this directory? If not DICOM inbox it
# 4. Is there BIDS for this directory? If not heudiconv it
# For steps 3 and 4 - allow for an overwrite
def main():

    parser = argparse.ArgumentParser(
        description='Import ADNI DICOM to NOTEPAD XNAT')
    parser.add_argument('--in_path', type=str,
                        required=True,
                        help='Path to subject to upload')
    parser.add_argument('--mr_study', type=str,
                        required=True,
                        help='Location of spreadsheet with study info for visits with MR data')
    parser.add_argument('--mr_image', type=str,
                        required=True,
                        help='Location of spreadsheet with image info for visits with MR data')
    parser.add_argument('--pet_study', type=str,
                        required=True,
                        help='Location of spreadsheet with study info for visits with PET data')
    parser.add_argument('--pet_image', type=str,
                        required=True,
                        help='Location of spreadsheet with image info for visits with PET data')
    parser.add_argument('--update',action='store_true',
                        help='Update existing records if already on XNAT')
    args = parser.parse_args()


    # Parse path to get subject ID and image ID 
    in_path = Path(args.in_path)
    adni_subject_id = extract_from_path(in_path,subject_id_pattern)

    if adni_subject_id is None:
        print("Could not identify subject from path")
        print(in_path)
        sys.exit(1)


    print(f'Subject {adni_subject_id}')    

    # Load in the data from the info sheet
    df_mr_info = process_study_sheet(args.mr_study)
    df_pet_info = process_study_sheet(args.pet_study)


    df_mr_image = process_image_sheet(args.mr_image,modality='MR')
    df_pet_image = process_image_sheet(args.pet_image,modality='PT')
    
    # Now merge the two
    df_mr_info = pd.merge(df_mr_info,df_mr_image,
                    left_on=['subject_id','visit'],
                    right_on=['subject_id','image_visit'],
                    how='outer')
    df_mr_info = df_mr_info.set_index('image_id')

    df_pet_info = pd.merge(df_pet_info,df_pet_image,
                    left_on=['subject_id','visit'],
                    right_on=['subject_id','image_visit'],
                    how='outer')
    df_pet_info = df_pet_info.set_index('image_id')

    # Find the rows that matches the subject and scan
    df_subject = df_mr_info.loc[df_mr_info['subject_id']==adni_subject_id]
    df_subject_demog = df_subject.dropna(subset='PTDOBYY')
    yob_constant = df_subject_demog['PTDOBYY'].all()
    gender_constant = df_subject_demog['PTGENDER'].all()
    ethnicity_constant = df_subject_demog['PTETHCAT'].all()
    race_constant = df_subject_demog['PTRACCAT'].all()
    education_constant = df_subject_demog['PTEDUCAT'].all()
    if not yob_constant or not gender_constant or \
        not ethnicity_constant or not race_constant or \
        not education_constant:
            print("Inconsistent subject level variables")
            print(df_subject_demog.loc[:,
                ["subject_id","visit","PTDOBYY",
                "PTEDUCAT","PTGENDER","PTETHCAT",
                "PTRACCAT"]
            ])

    # Unless I get a bunch of these, I'm going to assume
    # first row is OK
    # This information will be included when creating the subject
    first_row = df_subject_demog.iloc[0]
    print(first_row)
    in_yob = first_row['PTDOBYY']
    in_gender = first_row['PTGENDER_STR']
    in_ethnicity = first_row['PTETHCAT_STR']
    in_race = first_row['PTRACCAT_STR']
    in_education = first_row['PTEDUCAT']
    in_apoe = None
    if first_row['GENOTYPE'] != "NaN":
        print("Missing APOE Genotype")
    else:
        in_apoe = first_row['GENOTYPE'].replace("/","_")

    update_subject=args.update
    with xnat.connect(xnat_host) as xnat_session:
        # Get list of subjects for the project. 
        xnat_project = xnat_session.projects[notepad_project]
        xnat_subjects = xnat_project.subjects
        # If we don't have the subject in XNAT create it
        if adni_subject_id not in xnat_subjects:
            # This needs key demographics
            print(f"Creating subject {adni_subject_id}")
            xnat_subject = xnat_session.classes.SubjectData(
                parent=xnat_project, 
                label=adni_subject_id)
            update_subject=True
        else:
            xnat_subject = xnat_subjects[adni_subject_id]
        # If just created or args say to update it
        # Grab the metadata
        if update_subject:
            xnat_subject.demographics.yob = in_yob
            xnat_subject.demographics.gender = in_gender
            xnat_subject.demographics.ethnicity = in_ethnicity
            xnat_subject.demographics.education=in_education
            xnat_subject.demographics.race = in_race
            # This command will have to happen after upgrade or via REST call 
            if in_apoe is not None:
                apoe_string = {
                    "xnat:subjectData/fields/field[name=apoe]/field": in_apoe
                    }   
                xnat_session.put(
                    path=f"/data/projects/{notepad_project}/subjects/{adni_subject_id}",
                    query=apoe_string
                    )

    # Now we need to identify:
    # What images are DICOM and what are Nifti
    # Which ones are PET and which ones are MRI
    adni_image_id_list = []
    get_image_ids(in_path=in_path, path_glob='**/*.nii.gz', id_list=adni_image_id_list)
    get_image_ids(in_path=in_path, path_glob='**/*.dcm', id_list=adni_image_id_list)
        
    if not adni_image_id_list:
        print("Could not identify any images from paths")
        print(in_path)
        sys.exit(1)

    # So this should be a tree
    # STUDY (i.e. MR or PET session in XNAT)
    # Series (i.e. SCAN entry in XNAT)
    # Image (i.e. RESOURCE in a SCAN - DICOM or processed)
    # Upload_dict is a dict of ADNI study IDs[]
    # Study ID can have one to many series IDS
    # Series can have 

    # Go through all of the paths and find out what needs to be added
    upload_studies = {}

    dcm_files = in_path.glob('**/*.dcm')
    process_image_list(adni_subject_id,dcm_files,upload_studies,
                       df_mr_info,df_pet_info,dcm_flag=True)

    nii_files = in_path.glob('**/*.nii.gz')
    process_image_list(adni_subject_id,nii_files,upload_studies,
                       df_mr_info,df_pet_info,dcm_flag=False)
 
    with xnat.connect(xnat_host) as xnat_session:
        # Get list of subjects for the project. 
        xnat_project = xnat_session.projects[notepad_project]
        xnat_subject = xnat_project.subjects[adni_subject_id]

        xnat_img_sessions = xnat_subject.experiments
        # Go through all of the entries in the dictionary
        for study_id, study_info in upload_studies.items():
            xnat_session_label = study_info['session_id']
            print(xnat_session_label)
            print(study_info['image_date'])
            
            # If a session is not present it needs to be created
            # in part by archive_session
            xnat_image_session=None
            if xnat_session_label not in xnat_img_sessions:
                print(f"New session {xnat_session_label}")
                # Go through all of the series
                # Collecting the DICOM to upload
                series_map = study_info['series_list']
                n_total_dcm = 0
                study_dcm_list = []
                for series_id,series_info in series_map.items():
                    print(f"Series ID: {series_id}")
                    image_map = series_info['image_list']
                    for image_id, image_info in image_map.items():
                        n_dcm = len(image_info['dcm_files'])
                        print(f"DICOM Files: {n_dcm}")
                        # Concatenate all study files to one list
                        if n_dcm > 0:
                            n_total_dcm = n_total_dcm + n_dcm
                            study_dcm_list = study_dcm_list + image_info['dcm_files']
                if n_total_dcm > 0:
                    print(f"Total DICOM files: {n_total_dcm}")
                    zip_path = make_dcm_zip(study_dcm_list,
                                            study_id)
                    archive_session = xnat_session.services.import_(
                                zip_path, project=notepad_project, 
                                subject=adni_subject_id,
                                experiment=xnat_session_label)
                    for f in study_dcm_list:
                        f.unlink()
            else:
                print(f"Session {xnat_session_label} already archived")
        # Now add NIFTIs to existing sessions
        print("DICOM uploaded. Brief pause to let session archive")
        time.sleep(20)
        xnat_subject.clearcache()
        xnat_img_sessions = xnat_subject.experiments
        for study_id, study_info in upload_studies.items():
            xnat_session_label = study_info['session_id']
            print(xnat_session_label)
            print(study_info['image_date'])
            
            # If a session is not present it needs to be created
            # in part by archive_session
            xnat_image_session=None
            if xnat_session_label in xnat_img_sessions:
                xnat_image_session = xnat_img_sessions[xnat_session_label]
                series_map = study_info['series_list']
                for series_id,series_info in series_map.items():
                    print(f"Series ID: {series_id}")
                    scan_label = str(series_info['scan_number'])
                    print(scan_label)
                    if scan_label in xnat_image_session.scans:
                        print("Branding Series ID in scan")
                        xnat_scan = xnat_image_session.scans[scan_label]
                        xnat_scan.note = f"ADNI Series {series_id}"
                    image_map = series_info['image_list']
                    for image_id, image_info in image_map.items():
                        n_nii = len(image_info['nii_files'])
                        print(f"NII Files: {n_nii}")
                        # For NIFTIs only upload when there is an established scan there
                        if n_nii > 0:
                            # We are only uploading data where DICOM is available
                            # So the session exists and the scan does too
                            for nii in image_info['nii_files']:
                                if scan_label in xnat_image_session.scans:
                                    xnat_scan = xnat_image_session.scans[scan_label]
                                    image_description = image_info['image_description']
                                    if image_description in xnat_scan.resources:
                                        xnat_resource = xnat_scan.resources[image_description]
                                    else:
                                        xnat_resource = xnat_session.classes.ResourceCatalog(
                                            parent=xnat_scan, 
                                            label=image_description)
                                    print(f"Uploading Nifti to {xnat_resource}")
                                    xnat_resource.upload(str(nii), nii.name)
                                    nii.unlink()
                                    
        
if __name__ == "__main__":
    main()
 
