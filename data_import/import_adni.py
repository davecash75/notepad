import sys
import re
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
subject_id_pattern = re.compile(r"^\d{3}_S_\d{4}$")
image_id_pattern = re.compile(r"^I\d+$")
file_pattern = re.compile(r"^ADNI_(\d{3}_S_\d{4})_.*_S(\d+)_I(\d+).[dn].*")

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

# Make a main here

# This just extracts the bit of path that matches the pattern
# and returns it
def extract_from_path(input_path,input_pattern):
    out_key = None 
    # Parse the path to get the subject ID and image ID
    path_parts = input_path.parts
    for p in path_parts:
        if re.match(input_pattern,p):
            out_key = p
            return out_key

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
                    'mri_date': 'image_date'}
            )        
        # Keeping only 3T data (some rando scans with field strength 2.89)
        # And all of the MPRAGE have slice thicknesses less than 1.3
        df_image = df_image.loc[df_image["mri_field_str"]>2.5]
        #df_image = df_image.loc[df_image["mri_thickness"]<1.3]

    else:
        df_image = df_image[pet_keep_cols]
        df_image = df_image.rename(
            columns={'pet_visit': 'image_visit',
                    'pet_date': 'image_date'}
            )
        # Remove FDG and PIB (for time being)
        df_image = df_image.loc[df_image["pet_radiopharm"]!="18F-FDG"]
        df_image = df_image.loc[df_image["pet_radiopharm"]!="11C-PIB"]
    df_image = df_image.sort_values(by=['subject_id','image_date'])
    return df_image

def make_dcm_zip(dcm_list,study_id):
    zip_path = Path('/tmp',f'{study_id}.zip')
    study_uids = []
    series_uids = []
    make_new_uid = False
    for dcm_up in dcm_list:
        ds = dcm.dcmread(dcm_up)
        study_uids.append(ds.StudyInstanceUID)
        series_uids.append(ds.SeriesInstanceUID)
    study_uid_set = set(study_uids)
    series_uid_set = set(series_uids)
    if len(study_uid_set) > 1:
        print('Multiple UIDs detected')
        print(study_uid_set)
        make_new_uid=True
        temp_dcm_dir=Path('/tmp',f'{study_id}_temp')
        temp_dcm_dir.mkdir(exist_ok=True,parents=True)
        new_study_uid = dcm.uid.generate_uid()
        for dcm_up in dcm_list:
            ds = dcm.dcmread(dcm_up)
            ds.StudyInstanceUID = new_study_uid
            dcm_out = temp_dcm_dir / dcm_up.name
            ds.save_as(dcm_out)
      
    with ZipFile(zip_path,'w') as import_zip:
        if make_new_uid:
            for dcm_up in temp_dcm_dir.glob('*.dcm'):
                import_zip.write(dcm_up)
        else:
            for dcm_up in dcm_list:
                import_zip.write(dcm_up)

    if make_new_uid:
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
    subject_id = extract_from_path(in_path,subject_id_pattern)

    if subject_id is None:
        print("Could not identify subject from path")
        print(in_path)
        sys.exit(1)

    # Now we need to identify:
    # What images are DICOM and what are Nifti
    # Which ones are PET and which ones are MRI
    image_id_list = []

    nii_files = in_path.glob('**/*.nii.gz')
    nii_paths = set([x.parent for x in nii_files])
    print(nii_paths)
    for p in nii_paths:
        image_id = extract_from_path(p,image_id_pattern)
        image_id = int(image_id.replace('I',''))
        image_id_list.append(image_id)


    dcm_files = in_path.glob('**/*.dcm')
    # From the files get the directories 
    # and make a set of unique paths
    dcm_paths = set([x.parent for x in dcm_files])
    print(dcm_paths)
    # Get the image_id from each of the paths
    for p in dcm_paths:
        image_id = extract_from_path(p,image_id_pattern)
        if image_id not in image_id_list:
            image_id = int(image_id.replace('I',''))
            image_id_list.append(image_id)
        
        
    if not image_id_list:
        print("Could not identify any images from paths")
        print(in_path)
        sys.exit(1)

    print(f'Subject {subject_id}')    

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
    df_subject = df_mr_info.loc[df_mr_info['subject_id']==subject_id]
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
    first_row = df_subject_demog.iloc[0]
    print(first_row)
    in_yob = first_row['PTDOBYY']
    in_gender = first_row['PTGENDER_STR']
    in_ethnicity = first_row['PTETHCAT_STR']
    in_race = first_row['PTRACCAT_STR']
    in_education = first_row['PTEDUCAT']
    in_apoe = first_row['GENOTYPE'].replace("/","_")

    # Now here is where I want to assign all of the DICOMS
    # And all of the Niftis to the appropriate session
    # I envision a list of dictionaries
    # Each list entry will have the xnat session to be named
    # The ADNI session ID, the modality
    # And a list of all of the DICOM and Nifti files associated
    # with the session
    # That way I can zip up the DICOM and then import
    # The Nifti
    # First populate the image_id info
    upload_dict = {}
    image_to_study_map={}
    for image_id in image_id_list:
        session_id = None
        if image_id in df_mr_info.index:
            df_session = df_mr_info.loc[image_id].squeeze()
            visit_id = df_session['visit']
            study_id = df_session['study_id']
            image_date = df_session['image_date']
            session_id = f"{subject_id}-{visit_id}-MR"

        elif image_id in df_pet_info.index:
            df_session = df_pet_info.loc[image_id].squeeze()
            visit_id = df_session['visit']
            study_id = df_session['study_id']
            image_date = df_session['image_date']
            radiopharm = df_session['pet_radiopharm'].replace('18F-','')
            session_id = f"{subject_id}-{visit_id}-PET-{radiopharm}"
            
        if session_id is None:
            print(f'WARNING: Could not find {image_id} in the spreadsheets')
            print('Skipping this session for now')
            continue
        
        image_to_study_map[image_id] = int(study_id)
        upload_dict[int(study_id)] = {
            'visit_id': visit_id,
            'image_date': image_date,
            'session_id': session_id,
            'dcm_files': [],
            'nii_files': [],
        }
    dcm_files = in_path.glob('**/*.dcm')
    for f in dcm_files:
        # Parse file name:
        file_info = re.match(file_pattern,f.name)
        image_id = int(file_info.group(3))
        study_id = image_to_study_map[image_id]
        upload_dict[study_id]['dcm_files'].append(f)

    nii_files = in_path.glob('**/*.nii.gz')
    for f in nii_files:
        # Parse file name:
        file_info = re.match(file_pattern,f.name)
        image_id = int(file_info.group(3))
        study_id = image_to_study_map[image_id]
        upload_dict[study_id]['nii_files'].append(f)
        
    print(f'Subject: {subject_id}')
    
    update_subject=args.update
    with xnat.connect(xnat_host, extension_types=False) as xnat_session:
        # Get list of subjects for the project. 
        xnat_project = xnat_session.projects[notepad_project]
        xnat_subjects = xnat_project.subjects
        # If we don't have the subject in XNAT create it
        if subject_id not in xnat_subjects:
            # This needs key demographics
            print(f"Creating subject {subject_id}")
            xnat_subject = xnat_session.classes.SubjectData(
                parent=xnat_project, 
                label=subject_id)
            update_subject=True
        else:
            xnat_subject = xnat_subjects[subject_id]
        # If just created or args say to update it
        # Grab the metadata
        if update_subject:
            xnat_subject.demographics.yob = in_yob
            xnat_subject.demographics.gender = in_gender
            xnat_subject.demographics.ethnicity = in_ethnicity
            xnat_subject.demographics.education=in_education
            xnat_subject.demographics.race = in_race
            # This command will have to happen after upgrade or via REST call 
            apoe_string = {
                "xnat:subjectData/fields/field[name=apoe]/field": in_apoe
            }   
            xnat_session.put(
                path=f"/data/projects/{notepad_project}/subjects/{subject_id}",
                query=apoe_string
                )

        xnat_img_sessions = xnat_subjects[subject_id].experiments
        # Go through the sessions
        for study_id, upload_session in upload_dict.items():
            # If the data is not in the system, upload the DICOM!
            session_id = upload_session['session_id']
            if session_id not in xnat_img_sessions:
                print(f"New session {session_id}")
                n_dcm = len(upload_session['dcm_files'])
                n_nii = len(upload_session['nii_files'])
                print(f"Visit Type: {visit_id}")
                print(f"DICOM Files: {n_dcm}")
                print(f"NII Files: {n_nii}")
                if n_dcm > 0:
                    zip_path = make_dcm_zip(upload_session['dcm_files'],study_id)
                    archive_session = xnat_session.services.import_(
                        zip_path, project=notepad_project, 
                        subject=subject_id,
                        experiment=session_id)
            else:
                print(f"{session_id} already in XNAT")
                
        # IMPORT SESSION
        # Once the XNAT is updated, we can use DICOM Inbox
        # xnat_session.services.import_dicom_inbox(
        #    path=in_path,
        #    project=notepad_project,
        #    subject=subject_id,
        #    experiment=in_session,
        #)
                        
        
        # TO DO: BIDS CONVERT
        # This will need the file archived. 
        # Will wait to see that DICOM is sorted before adding
        
        
if __name__ == "__main__":
    main()
 
