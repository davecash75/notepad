import argparse
import sys
from pathlib import Path
from collections import namedtuple
import pandas as pd
import json
import xnat

# Some helpful globals
# Host for the xnat where data is going
xnat_host = "https://xnat-srv.drc.ion.ucl.ac.uk"
# Project for data
notepad_project = "NOTEPAD_WRAP"

gender_map = {
    1 : 'male',
    2 : 'female'
}

race_map = {
    1: "White",
    2: "Black or African American",
    4: "American Indian or Alaskan Native",
    5: "Asian",
    6: "Native Hawaiian or Other Pacific Islander",
    7: "Other",
    8: "Unknown",
    100: "More than one race",
}

CogScores = namedtuple("CogScores",["Visit","CDR_Global","CDR_Sum","MMSE"])


def bids_extract(data,key,default):
    output = default
    if key in data:
        output = str(data[key])
    return(output)

def create_subject(session, project, subject_label,df_subject):
    if subject_label in project.subjects:
        print("Subject already in project")
        return(project.subjects[subject_label])
    elif subject_label not in df_subject.index:
        print("This subject ID not in main subject info spreadsheet")
        return None
    else:
        print("Creating Subject")
        df_subject_info = df_subject.loc[subject_label].squeeze()
        in_age = df_subject_info['Age_At_Baseline']
        in_gender = df_subject_info['SEX_STR']
        in_ethnicity = df_subject_info['ETHNIC_STR']
        in_education = df_subject_info['EducYrs']
        in_race = df_subject_info['RACE_STR']
        in_apoe = str(df_subject_info['APOEGN'])
        subject = session.classes.SubjectData(
                parent=project, 
                label=subject_label)
            
        if str(in_age) != "nan":
            subject.demographics.age = in_age
        if str(in_gender) != "nan":
            subject.demographics.gender = in_gender
        if str(in_ethnicity) != "nan":
            subject.demographics.ethnicity = in_ethnicity
        if str(in_education) != "nan":
            subject.demographics.education=in_education if in_education <=30 else 30
        if str(in_race) != "nan":
            subject.demographics.race = in_race
        if in_apoe == "nan":
            in_apoe = "NA"
        else:
            in_apoe = in_apoe.replace('E','')
            in_apoe = in_apoe.replace('/','_')
            
        # This command will have to happen after upgrade or via REST call 
        var_string = {
            "xnat:subjectData/fields/field[name=apoe]/field": in_apoe,
            "xnat:subjectData/fields/field[name=group]/field": in_group,
        }   
        session.put(
            path=f"/data/projects/{notepad_project}/subjects/{subject_label}",
            query=var_string
            )
        return(subject)

def find_cog_scores(subject,age,df_visits,df_cdr, df_mmse):
    df_subject_visits = df_visits.loc[subject,:]
    df_subject_visits['diff_to_scan'] = df_subject_visits['Age_At_Visit'] - age
    # Modified from Source - https://stackoverflow.com/a
    # Posted by Zero, modified by community. See post 'Timeline' for change history
    # Retrieved 2025-11-17, License - CC BY-SA 4.0
    df_closest_visit = df_subject_visits.iloc[df_subject_visits['diff_to_scan'].abs().argsort()[:1]].squeeze()
    closest_visit = df_closest_visit['VisNo']
    print("Closest visit")
    print(f"Visit Number: {closest_visit}")
    print(f"Age At Visit: {df_closest_visit['Age_At_Visit']}")
    print(f"Diff From Image: {df_closest_visit['diff_to_scan']}")
    df_cdr_subject = df_cdr.loc[subject,:]
    df_cdr_subject = df_cdr_subject.set_index('VisNo')
    cdr_global = df_cdr_subject[closest_visit,'CDRRating']
    cdr_sum = df_cdr_subject[closest_visit,'SumOfBoxes']
    
    df_mmse_subject = df_mmse.loc[subject,:]
    df_mmse_subject = df_mmse_subject.set_index('VisNo')
    mmse = df_mmse_subject[closest_visit,'mmseTot']
    output_cog = CogScores(closest_visit,cdr_global,cdr_sum,mmse)
    return(output_cog)
    
    

def create_experiment(session,subject,modality,
                      experiment_label,
                      nii_file,json_file,
                      cog_outcomes):
    resource="BIDS"
    subject_label=subject.label
    #Read in JSON
    if not json_file.exists():
        return(None)
    with open(json_file,'r') as sidecar:
        bids_data = json.load(sidecar)

    if experiment_label in subject.experiments:
        print("Subject already in project")
        xnat_experiment = subject.experiments[experiment_label]
    else:
        print(f"Creating Session {experiment_label}")
        if modality == "MR":
            xnat_experiment = session.classes.MrSessionData(
                    parent=subject, label=experiment_label)
            xnat_experiment.field_strength = bids_extract(bids_data,
                                                        'MagneticFieldStrength',
                                                        'Not specified')
            var_string = {
                "xnat:mrSessionData/fields/field[name=visitlabel]/field": visit_label,
                "xnat:mrSessionData/fields/field[name=daysfromrandomisation]/field": str(days_to_random),
                "xnat:mrSessionData/fields/field[name=mmse]/field": cog_outcomes.MMSE,
                "xnat:mrSessionData/fields/field[name=cdrsob]/field": cog_outcomes.CDR_Sum,
                "xnat:mrSessionData/fields/field[name=cdrglobal]/field": cog_outcomes.CDR_Global,
                }   
            session.put(
                path=f"/data/projects/{notepad_project}/subjects/{subject_label}/experiments/{experiment_label}",
                query=var_string
            )
        else:
            xnat_experiment = session.classes.PetSessionData(
                parent=subject, label=experiment_label)
            xnat_experiment.tracer.name = bids_extract(
                bids_data,
                'Radiopharmaceutical',
                'Unknown')
            xnat_experiment.tracer.data['dose'] = bids_extract(
                bids_data,
                'InjectedRadioactivity',
                '0.0'
            )
        xnat_experiment.manufacturer = bids_extract(
            bids_data,
            'Manufacturer',
            'Unknown'
            )

    # Get series Number
    series_number = bids_extract(bids_data,"SeriesNumber",3)

    xnat_scan = None
    # Now that the session is created, 
    # let's check if the scan
    # is in the experiment
    if xnat_experiment.scans:
        if series_number in xnat_experiment.scans:
            # This data has already been uploaded
            # So we can move it to the uploaded path
            if nii_file.exists():
                nii_new_path = nii_file.parent / 'uploaded' / nii_file.name
                nii_file.rename(nii_new_path)
            else:   
                print(f"[WARNING] Could not find file: {nii_file}")
            if json_file.exists():
                #Move to uploaded path when done
                json_new_path = json_file.parent / 'uploaded' / json_file.name
                json_file.rename(json_new_path)
            else:
                print(f"[WARNING] Could not find file: {json_file}")
            xnat_scan = xnat_experiment.scans[series_number]
    
    # Create a scan if not available
    if xnat_scan is None:
        # Can't really do much if no Nifti file present
        if nii_file.exists():
            slice_thickness = bids_extract(bids_data,
                                        'SliceThickness',
                                        '0.0')
            if modality == "MR":
                series_description = bids_extract(
                    bids_data,
                    "SeriesDescription",
                    "T1"
                    )
                xnat_scan = session.classes.MrScanData(
                    parent=xnat_experiment, 
                    id=series_number, 
                    type=series_description, 
                    series_description=series_description
                    )
                xnat_scan.parameters.te = bids_extract(
                    bids_data,
                    'EchoTime',
                    '0.0'
                    )
                xnat_scan.parameters.tr = bids_extract(
                    bids_data,
                    'RepetitionTime',
                    '0.0'
                    )
                xnat_scan.parameters.ti = bids_extract(
                    bids_data,
                    'InversionTime',
                    '0.0'
                    )
            else:
                series_description = bids_extract(
                    bids_data,
                    "SeriesDescription",
                    "PET AC"
                    )
                xnat_scan = session.classes.PetScanData(
                        parent=xnat_experiment, 
                        id=series_number, 
                        type=series_description, 
                        series_description=series_description
                        )
            
            xnat_resource = None
            if xnat_scan.resources:
                if resource in xnat_scan.resources:
                    xnat_resource = xnat_scan.resources[resource]
            if xnat_resource is None:
                xnat_resource = session.classes.ResourceCatalog(
                    parent=xnat_scan, 
                    label=resource
                    )
            xnat_resource.upload(str(nii_file), nii_file.name)
            # Move to uploaded path when done
            nii_new_path = nii_file.parent / 'uploaded' / nii_file.name
            nii_file.rename(nii_new_path)

            xnat_resource.upload(str(json_file), json_file.name)
            #Move to uploaded path when done
            json_new_path = json_file.parent / 'uploaded' / json_file.name
            json_file.rename(json_new_path)
        else:   
            print(f"[WARNING] Could not find file: {nii_file}")
    return(xnat_experiment)


def main():
    parser = argparse.ArgumentParser(
            description='Import WRAP to NOTEPAD XNAT')
    parser.add_argument('--in_path', type=str,
                    required=True,
                    help='Path to data')
    parser.add_argument("--stop", default=-1, type=int, help="Number of scans to start. Default is -1 which means do them all")
    parser.add_argument("--start", default=0, type=int, help="session type (CT/MR)")
    args = parser.parse_args()

    in_dir=Path(args.in_path)
    done_dir = in_dir / 'uploaded'
    done_dir.mkdir(parents=True,exist_ok=True)
    max_i = args.stop
    start_i = args.start
    i=0

    # Read in key spreadsheets
    subject_info_sheet = in_dir / 'Data' / 'Demographics.csv'
    df_subject = pd.read_csv(subject_info_sheet)
    # Set index to BID for quick indexing
    df_subject = df_subject.set_index('wrapnum')
    df_subject['RACE_STR'] = df_subject['race1'].map(race_map)
    df_subject.loc[pd.notna(df_subject['race2']),'RACE_STR'] = "More than one race"
    df_subject['ETHNIC_STR'] = "Not Hispanic or Latino"
    df_subject.loc[df_subject['hispanic_or_latino']>1,'ETHNIC_STR'] = "Hispanic or Latino"
    df_subject.loc[df_subject['hispanic_or_latino'].isna(),'ETHNIC_STR'] = "Unknown"
    df_subject['SEX_STR'] = df_subject['SEX'].map(gender_map)

    visit_info_sheet = in_dir / 'Data' / 'fqryStatisticalData.csv'
    df_visit = pd.read_csv(visit_info_sheet)
    df_visit = df_visit.sort_values(by=['wrapnum','VisNo'])
    df_visit = df_visit.set_index('wrapnum')
    df_visit['Age_At_Visit'] = df_visit['Age_At_Baseline'] + \
        (df_visit['Days_Since_Baseline'] / 365.25)
    
    df_subject = df_subject.merge(df_visit, how="left",
                                  left_index=True,
                                  right_index=True,
                                  validate="one_to_many") 

    cdr_sheet = in_dir / 'Data' / 'CDR.csv'
    df_cdr = pd.read_csv(cdr_sheet)
    df_cdr = df_cdr.sort_values(by=['wrapnum','VisNo'])
    df_cdr = df_cdr.set_index('wrapnum')
    df_cdr = df_cdr.loc[:,
                        ['VisNo',
                         'SumOfBoxes',
                         'CDRRating',
                         'estimated_questionnaire_days_after_baseline']]
    
    mmse_sheet = in_dir / 'Data' / 'NeuropsychScores.csv'
    df_mmse = pd.read_csv(mmse_sheet)
    df_mmse = df_mmse.sort_values(by=['wrapnum','VisNo'])
    df_mmse = df_mmse.set_index('wrapnum')
    df_mmse = df_mmse.loc[:,['VisNo','mmseTot']]
    
    apoe_sheet = in_dir / 'Data' / 'APG.csv'
    df_apoe = pd.read_csv(apoe_sheet)
    df_apoe = df_apoe.set_index('wrapnum')
    df_apoe = df_apoe.loc[:,['all1','all2']]
    df_apoe['APOEGN'] = df_apoe['all1'] + "_" + df_apoe['all2']
    df_subject = df_subject.merge(df_apoe, how="left",
                                  left_index=True,
                                  right_index=True,
                                  validate="one_to_one") 

    
    wrap_scans = in_dir.rglob('*.json')
    with xnat.connect(xnat_host) as xnat_session:
        xnat_project = xnat_session.projects[notepad_project]

        for json_path in sorted(wrap_scans):
            if i < start_i:
                i=i+1
                continue
            print(f"{i} - {json_path.name}")
            json_name = str(json_path)
            # Check to see if there is both a JSON and a GZIPPED NII        
            nii_name = json_name.replace('.json','.nii.gz')
            nii_path = Path(nii_name)
            if not nii_path.exists():
                print('This is not a complete set, the nifti file is missing')
                continue
            # The file names look pretty sensible, delineated by _
            # First split: Subject ID (sub-wrap02020)
            # Secont split: Visit Code, really ses_age (ses-060)
            # Third split: MOdality information (T1, FLAIR, tracer for PET)
            image_parts = json_path.stem.split('_')
            subject_id = image_parts[0].replace('sub-','')
            scan_age = image_parts[1].replace('ses-','')
            if 'trc-' in image_parts[2]:
                modality = "PET"
                image_type = image_parts[2].replace('trc-','')
            else:
                modality = "MR"
                image_type = image_parts[2]
            print(f"Subject ID: {subject_id}")
            print(f"Visit ID: {scan_age}")
            print(f"Modality: {modality}")
            print(f"Image: {image_type}")
            
            # Create subject
            xnat_subject = create_subject(xnat_session,
                                          xnat_project,
                                          subject_id,
                                          df_subject)

            cdr_sum = '-1'
            cdr_global = 'NA'
            mmse = '-1'
            cog_values = find_cog_scores(
                subject_id,
                scan_age, 
                df_visit,
                df_cdr,
                df_mmse
                )
            
            if modality=="PET":
                radiopharm = image_type.replace("11CPiB","PIB")
                radiopharm = image_type.replace("18FMK6240","MK6240")
                radiopharm = image_type.replace("18FNAV4694","NAV4694")
                experiment_id = f"{subject_id}-v{cog_values.Visit}-{modality}-{radiopharm}"
            else:
                experiment_id = f"{subject_id}-v{cog_values.Visit}-{modality}"
                
            if xnat_subject is not None:
                experiment = create_experiment(xnat_session,
                                               xnat_subject,
                                               modality,
                                               experiment_id,
                                               nii_path,
                                               json_path,
                                               cog_values)
            if i >= max_i and max_i > 0:
                print("Hit stopping condition")
                sys.exit(1)
            else:
                i=i+1

        
if __name__ == "__main__":
    main()
