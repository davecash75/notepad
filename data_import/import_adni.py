import sys
import re
from pathlib import Path
import shutil
import argparse
import pandas as pd
import xnat 
import heudiconv

def extract_from_path(input_path,input_pattern):
    out_key = None 
    # Parse the path to get the subject ID and image ID
    path_parts = input_path.parts
    for p in path_parts:
        if re.match(input_pattern,p):
            out_key = p
            return out_key

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
parser = argparse.ArgumentParser(
    description='Import ADNI DICOM to NOTEPAD XNAT')
parser.add_argument('--in_path', type=str,
                    required=True,
                    help='Path to session')
parser.add_argument('--in_study', type=str,
                    required=True,
                    help='Location of spreadsheet with study info')
parser.add_argument('--in_image', type=str,
                    required=True,
                    help='Location of spreadsheet with image info')
parser.add_argument('--modality',type=str,
                    choices=['MR','PET'],default='MR',
                    help='Choose which imaging values to use MR or PET')
parser.add_argument('--update',action='store_true',
                    help='Update existing records if already on XNAT')
args = parser.parse_args()

# Some helpful globals
xnat_host = "https://xnat-srv.drc.ion.ucl.ac.uk"
notepad_project = "NOTEPAD_ADNI"

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

# Parse path to get subject ID and image ID 
in_path = Path(args.in_path)
subject_id_pattern = re.compile(r"^\d{3}_S_\d{4}$")
subject_id = extract_from_path(in_path,subject_id_pattern)

if subject_id is None:
    print("Could not identify subject from path")
    print(in_path)
    sys.exit(1)

image_id_pattern = re.compile(r"^I\d+$")
image_id = extract_from_path(in_path,image_id_pattern)
    
if not image_id:
    print("Could not identify image from path")
    print(in_path)
    sys.exit(1)

image_id = int(image_id.replace('I',''))
print(f'Subject {subject_id}')
print(f'Image {image_id}')
    

in_study = args.in_study
in_image = args.in_image

# Load in the MRI data - it's a lot of lot of data
# So first we are only going to keep a handful of columns
df_image = pd.read_csv(in_image,
                     low_memory = False)
if (args.modality=='MR'):
    keep_cols = [
        'image_id','subject_id','study_id','mri_visit',
        'mri_date','mri_description','mri_thickness',
        'mri_mfr','mri_mfr_model','mri_field_str'
        ]
    df_image = df_image[keep_cols]
    df_image = df_image.rename(
        columns={'mri_visit': 'image_visit',
                 'mri_date': 'image_date'}
        )
    
    # Keeping only 3T data (some rando scans with field strength 2.89)
    # And all of the MPRAGE have slice thicknesses less than 1.3
    df_image = df_image.loc[df_image["mri_field_str"]>2.5]
    df_image = df_image.loc[df_image["mri_thickness"]<1.3]

else:
    keep_cols = [
        'image_id','subject_id','study_id','pet_visit',
        'pet_date','pet_description','pet_mfr',
        'pet_mfr_model','pet_radiopharm'
        ]
    df_image = df_image[keep_cols]
    df_image = df_image.rename(
        columns={'pet_visit': 'image_visit',
                 'pet_date': 'image_date'}
        )
    # Remove FDG and PIB (for time being)
    df_image = df_image.loc[df_image["pet_radiopharm"]!="18F-FDG"]
    df_image = df_image.loc[df_image["pet_radiopharm"]!="11C-PIB"]

# Load in the data from the info sheet
df_info = pd.read_csv(in_study)

# A bit of cleaning up on the racial category
# So that it will map properly
df_info.loc[df_info['PTRACCAT']=='9','PTRACCAT'] = 7
df_info.loc[df_info['PTRACCAT']=='1|4','PTRACCAT'] = 6
df_info.loc[df_info['PTRACCAT']=='1|5','PTRACCAT'] = 6
df_info.loc[df_info['PTRACCAT']=='4|5','PTRACCAT'] = 6
df_info.loc[df_info['PTRACCAT']=='2|4','PTRACCAT'] = 6
df_info.loc[df_info['PTRACCAT']=='3|4|5','PTRACCAT'] = 6
df_info['PTRACCAT'] = pd.to_numeric(df_info['PTRACCAT'])


df_info['PTETHCAT_STR'] = df_info['PTETHCAT'].map(ethnicity_map)
df_info['PTRACCAT_STR'] = df_info['PTRACCAT'].map(race_map)
df_info['PTGENDER_STR'] = df_info['PTGENDER'].map(gender_map)

# Now merge the two
df_info = pd.merge(df_info,df_image,
                   left_on=['subject_id','visit'],
                   right_on=['subject_id','image_visit'],
                   how='outer')


# Find the rows that matches the subject and scan
df_subject = df_info.loc[df_info['subject_id']==subject_id]
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

df_session = df_info.loc[(df_info['subject_id']==subject_id) & (df_info['image_id']==image_id) ].squeeze()
print(df_session)
visit_id = df_session['visit']
print(f'Subject: {subject_id}')
print(f'Visit: {visit_id}')
if args.modality == "MR":
    session_id = f"{subject_id}-{visit_id}-{args.modality}"
else:
    radiopharm = df_session['pet_radiopharm']
    session_id = f"{subject_id}-{visit_id}-{args.modality}-{radiopharm}"
    
# Only keeping the verify=False in temporarily for testing
with xnat.connect(xnat_host, extension_types=False, verify=False) as xnat_session:
    # Get list of subjects for the project. 
    xnat_project = xnat_session.projects[notepad_project]
    xnat_subjects = xnat_project.subjects
    if subject_id not in xnat_subjects:
        # This needs key demographics
        print(f"Creating subject {subject_id}")
        new_subject = xnat_session.classes.SubjectData(
            parent=xnat_project, 
            label=subject_id)
        new_subject.demographics.yob = in_yob
        new_subject.demographics.gender = in_gender
        new_subject.demographics.ethnicity = in_ethnicity
        new_subject.demographics.education=in_education
        new_subject.demographics.race = in_race
        # This command will have to happen after upgrade or via REST call 
        apoe_string = {
            "xnat:subjectData/fields/field[name=apoe]/field": in_apoe
        }   
        xnat_session.put(
            path=f"/data/projects/{notepad_project}/subjects/{subject_id}",
            query=apoe_string
            )
        # new_subject.custom_variables['default']['APOE'] = in_apoe
    elif args.update:
        update_subject = xnat_subjects[subject_id]
        update_subject.demographics.yob = in_yob
        update_subject.demographics.gender = in_gender
        update_subject.demographics.ethnicity = in_ethnicity
        update_subject.demographics.education=in_education
        update_subject.demographics.race = in_race
        # This command will have to happen after upgrade or via REST call 
        apoe_string = {
            "xnat:subjectData/fields/field[name=apoe]/field": in_apoe
        }   
        xnat_session.put(
            path=f"/data/projects/{notepad_project}/subjects/{subject_id}",
            query=apoe_string
            )
 

    # IMPORT SESSION
    # Need to generate a session tag from subject and visit
    # This command will have to happen after upgrade of XNAT
    # You may have to DICOM push these for the time being    
    #xnat_session.services.import_dicom_inbox(
    #    path=in_path,
    #    project=notepad_project,
    #    subject=subject_id,
    #    experiment=in_session,
    #)
    xnat_img_sessions = xnat_subjects[subject_id].experiments
    if session_id not in xnat_img_sessions:
        import_zip = shutil.make_archive(
            '/tmp/upload', format='zip', root_dir=str(in_path)
            )
        # Zip the path up to somewhere temporary
        prearchive_session = xnat_session.services.import_(
            import_zip, project=notepad_project, subject=subject_id,
            experiment=session_id,
            destination='/prearchive')
    
    # TO DO: BIDS CONVERT
    # This will need the file archived. 
    # Will wait to see that DICOM is sorted before adding
        
        
    
