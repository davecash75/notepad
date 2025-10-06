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
datetime_pattern = re.compile(r"^20\d{2}-[01]\d-[0123]\d_[012]\d_[0-5]\d")

# Which columns from ADNI MRI and PET spreadsheets should be kept
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
    7: "Unknown",
    8: "Native Hawaiian",
    9: "Other Pacific Islander"
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
    return (image_id, series_id, file_path)

def process_image_list(subject_id,image_list,adni_studies,
                       df_image_info,
                       dcm_flag=True):
    # Loop through the images found in the file
    image_file_info = []

    for f in image_list:
        # Extract the series ID and image ID from the filename
        image_series_id = parse_image_filename(f)
        image_file_info.append(image_series_id)
    # Take the list of tuples and turn them into a dataframe
    df_image_files = pd.DataFrame(
        image_file_info,
        columns=['image_id','series_id','file_path']
        )

    # So that we can drop the duplicates
    df_image_ids = df_image_files.drop_duplicates(
        subset=['image_id','series_id']
        )

    # We need all of the relevant info to upload these
    # So merge this with the combined image sheet
    df_images_to_process= pd.merge(df_image_ids,
                                   df_image_info,
                                   how='left',
                                   left_on='image_id',
                                   right_index=True,
                                   indicator=True
                                   )
    print(df_images_to_process)
    
    # So the images_to_process dataframe contains
    # All of the information for the files that are in the filelist
    # We may need to modify the image description to match paths
    # image_description = df_session['image_description']
    # image_description = image_description.replace(';','_')
    # image_description = image_description.replace(' ','_')
    # adni_info['image_description'] = image_description
    
    # If we don't have information for this study ID
    for img_row in df_images_to_process.itertuples():
        print(img_row)
        if img_row.study_id not in adni_studies:
            study_info = {
                'image_date': img_row.image_date,
                'session_id': img_row.session_label,
                'series_list' : {},
            }
            adni_studies[img_row.study_id] = study_info
        series_map = adni_studies[img_row.study_id]['series_list']
        if img_row.series_id not in series_map:
            xnat_scan_number = str(img_row.series_id)
            if dcm_flag:
                with dcm.dcmread(img_row.file_path) as ds:
                    if ds.SeriesNumber is not None:
                        xnat_scan_number = str(ds.SeriesNumber)
            series_info = {
                'scan_number': xnat_scan_number,
                'image_list':{},
                }
            print(img_row.series_id)
            print(series_info['scan_number'])
            series_map[img_row.series_id] = series_info
        image_map = series_map[img_row.series_id]['image_list']
        if img_row.image_id not in image_map:
            image_info = {
                'image_description': img_row.image_description,
                'dcm_files': [],
                'nii_files': [],
            }
            image_map[img_row.image_id] = image_info
        df_image_subset = df_image_files.loc[
            df_image_files['image_id']==img_row.image_id,'file_path'
            ]
        file_type = 'nii_files'
        if dcm_flag:        
            file_type = 'dcm_files'
        image_map[img_row.image_id][file_type] = df_image_subset.tolist()
            


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
    df_info.loc[df_info['PTRACCAT']=='5|8','PTRACCAT'] = 6
    df_info.loc[df_info['PTRACCAT']=='1|4|5','PTRACCAT'] = 6
    df_info.loc[df_info['PTRACCAT']=='3|4|5','PTRACCAT'] = 6
    df_info['PTRACCAT'] = pd.to_numeric(df_info['PTRACCAT'])
    df_info['PTETHCAT_STR'] = df_info['PTETHCAT'].map(ethnicity_map)
    df_info['PTRACCAT_STR'] = df_info['PTRACCAT'].map(race_map)
    df_info['PTGENDER_STR'] = df_info['PTGENDER'].map(gender_map)
    return df_info

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


    print(f"Subject {adni_subject_id}")    

    # Load in the data from the info sheet
    mri_info_path = Path(args.mri)
    df_mri = read_info_sheet(mri_info_path,
                             modality="MR")

    pet_info_path = Path(args.pet)
    df_pet = read_info_sheet(pet_info_path,
                             modality="PT")

    demog_path = Path(args.demog)
    df_demog = process_study_sheet(demog_path)

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
    df_mri['visit'] = df_mri['visit'].fillna("Unknown")
    df_mri["session_label"] = df_mri["subject_id"] + "-" + df_mri["visit"] + "-MR"
    df_mri = df_mri.set_index("session_label")
    df_mri = df_mri.sort_index()
    print("Unknown MRI visits")
    print(df_mri.loc[df_mri["visit"]=="Unknown"])

    df_pet = pd.merge(df_pet, df_demog,
                      on=['subject_id','image_visit'],
                      how='left')
    radiopharm = df_pet['pet_radiopharm'].str.replace('18F-','')

    df_pet['visit'] = df_pet['visit'].fillna("Unknown")
    df_pet["session_label"] = df_pet["subject_id"] + "-" + df_pet["visit"] + "-PET-" + radiopharm
    df_pet = df_pet.set_index("session_label")
    df_pet = df_pet.sort_index()

    print("Unknown PET visits")
    print(df_pet.loc[df_pet["visit"]=="Unknown"])

    # Concatenate the MRI and the PET into the same table
    df_images = pd.concat([df_mri,df_pet])
    df_images = df_images.reset_index()
    df_images = df_images.set_index('image_id')
    #print(df_images)
    subject_images = df_images.loc[df_images['subject_id']==adni_subject_id],['subject_id','image_id','visit','image_visit']
    #print(subject_images)
    # Find the rows that matches the subject and scan
    df_subject = df_demog.loc[df_demog['subject_id']==adni_subject_id]
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
    #print(first_row)
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
    if not dcm_files:
        print("No DICOM files found")
    else:
        process_image_list(
            adni_subject_id,
            dcm_files,
            upload_studies,
            df_images,
            dcm_flag=True
            )

    nii_files = in_path.glob('**/*.nii.gz')
    if not nii_files:
        print("No nifti files found")
    else:
        process_image_list(
            adni_subject_id,
            nii_files,
            upload_studies,
            df_images,
            dcm_flag=False
            )
 
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
                    try:
                        archive_session = xnat_session.services.import_(
                                    zip_path, project=notepad_project, 
                                    subject=adni_subject_id,
                                    experiment=xnat_session_label,
                                    content_type='application/zip')
                    except xnat.exceptions.XNATUploadError:
                        print("Error uploading ZIP file")
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
 
