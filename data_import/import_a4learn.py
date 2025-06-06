import argparse
import sys
from pathlib import Path
import pandas as pd
import json
import xnat

# Some helpful globals
# Host for the xnat where data is going
xnat_host = "https://xnat-srv.drc.ion.ucl.ac.uk"
# Project for data
notepad_project = "NOTEPAD_A4"

gender_map = {
    1 : 'female',
    2 : 'male'
}
ethnicity_map = {
    50: "Hispanic or Latino",
    56: "Not Hispanic or Latino",
    97: "Unknown"
}
race_map = {
    1: "White",
    2: "Black or African American",
    58: "Asian",
    79: "Native Hawaiian or Other Pacific Islander",
    84: "American Indian or Alaskan Native",
    97: "Unknown",
    100: "More than one race",
}

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
        in_age = df_subject_info['AGEYR']
        in_group = df_subject_info['SUBSTUDY']
        in_gender = df_subject_info['SEX_STR']
        in_ethnicity = df_subject_info['ETHNIC_STR']
        in_education = df_subject_info['EDCCNTU']
        in_race = df_subject_info['RACE_STR']
        in_apoe = df_subject_info['APOEGN']
        subject = session.classes.SubjectData(
                parent=project, 
                label=subject_label)
            
        subject.demographics.age = in_age
        subject.demographics.gender = in_gender
        subject.demographics.ethnicity = in_ethnicity
        subject.demographics.education=in_education
        subject.demographics.race = in_race
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


def create_experiment(session,subject,modality,
                      experiment_label,
                      nii_file,json_file,
                      visit_label,days_to_random,
                      cdr_sob = '-1',cdr_global = 'NA',mmse = '-1'):
    resource="BIDS"
    subject_label=subject.label
    if experiment_label in subject.experiments:
        print("Subject already in project")
        return(subject.experiments[experiment_label])
    else:
        print("Creating Session")
        #Read in JSON
        with open(json_file,'r') as sidecar:
            bids_data = json.load(sidecar)
        #Get series Number
        series_number = bids_extract(bids_data,"SeriesNumber",3)
        if modality == "MR":
            series_description = bids_extract(bids_data,
                                              "SeriesDescription",
                                              "T1")
            xnat_experiment = session.classes.MrSessionData(
                parent=subject, label=experiment_label)
            xnat_experiment.field_strength = bids_extract(bids_data,
                                                          'MagneticFieldStrength',
                                                          'Not specified')
            var_string = {
                "xnat:mrSessionData/fields/field[name=visitlabel]/field": visit_label,
                "xnat:mrSessionData/fields/field[name=daysfromrandomisation]/field": str(days_to_random),
                "xnat:mrSessionData/fields/field[name=mmse]/field": mmse,
                "xnat:mrSessionData/fields/field[name=cdrsob]/field": cdr_sob,
                "xnat:mrSessionData/fields/field[name=cdrglobal]/field": cdr_global,
                }   
            session.put(
                path=f"/data/projects/{notepad_project}/subjects/{subject_label}/experiments/{experiment_label}",
                query=var_string
            )
       

        else:
            series_description = bids_extract(bids_data,
                                              "SeriesDescription",
                                              "PET AC")
            xnat_experiment = session.classes.PetSessionData(
                parent=subject, label=experiment_label)
            xnat_experiment.tracer.name = bids_extract(bids_data,
                                                  'Radiopharmaceutical',
                                                  'Unknown')
            xnat_experiment.tracer.data['dose'] = bids_extract(
                bids_data,
                'InjectedRadioactivity',
                '0.0'
            )
            var_string = {
                "xnat:petSessionData/fields/field[name=visitlabel]/field": visit_label,
                "xnat:petSessionData/fields/field[name=daysfromrandomization]/field": days_to_random,
                "xnat:petSessionData/fields/field[name=mmse]/field": mmse,
                "xnat:petSessionData/fields/field[name=cdrsob]/field": cdr_sob,
                "xnat:petSessionData/fields/field[name=cdrglobal]/field": cdr_global,
                }   
            session.put(
                path=f"/data/projects/{notepad_project}/subjects/{subject_label}/experiments/{experiment_label}",
                query=var_string
            )
        # See if you can add some more important stuff here
        # For the sessions
        xnat_experiment.manufacturer = bids_extract(bids_data,
                                                    'Manufacturer',
                                                    'Unknown')
        slice_thickness = bids_extract(bids_data,
                                       'SliceThickness',
                                       '0.0')
        # If there isn't a scan we need to create one
        # If the scan isn't in the list, we need to create one
        xnat_scan = None
        if xnat_experiment.scans:
            if series_number in xnat_experiment.scans:
                xnat_scan = xnat_experiment.scans[series_number]
        if xnat_scan is None:
            if modality == "MR":
                xnat_scan = session.classes.MrScanData(
                    parent=xnat_experiment, 
                    id=series_number, 
                    type=series_description, 
                    series_description=series_description
                    )
                xnat_scan.parameters.te = bids_extract(bids_data,
                                                      'EchoTime',
                                                       '0.0')
                xnat_scan.parameters.tr = bids_extract(bids_data,
                                                       'RepetitionTime',
                                                       '0.0')
                xnat_scan.parameters.ti = bids_extract(bids_data,
                                                       'InversionTime',
                                                       '0.0')
            else:
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
                parent=xnat_scan, label=resource)
        if nii_file.exists():
            xnat_resource.upload(str(nii_file), nii_file.name)
            # Move to uploaded path when done
            nii_new_path = nii_file.parent / 'uploaded' / nii_file.name
            nii_file.rename(nii_new_path)
        else:   
            print(f"[WARNING] Could not find file: {nii_file}")
        if json_file.exists():
            xnat_resource.upload(str(json_file), json_file.name)
            #Move to uploaded path when done
            json_new_path = json_file.parent / 'uploaded' / json_file.name
            json_file.rename(json_new_path)
        else:
            print(f"[WARNING] Could not find file: {json_file}")
        var_string = {
            "xnat:subjectData/fields/field[name=VisitLabel]/field": visit_label,
            "xnat:subjectData/fields/field[name=DaysFromRandomisation]/field": days_to_random,
            }   
        session.put(
            path=f"/data/projects/{notepad_project}/subjects/{subject_label}/experiments/{experiment_label}",
            query=var_string
        )

    return(xnat_experiment)


def main():
    parser = argparse.ArgumentParser(
            description='Import A4/LEARN to NOTEPAD XNAT')
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
    subject_info_sheet = in_dir / 'SUBJINFO.csv'
    df_subject = pd.read_csv(subject_info_sheet)
    # Set index to BID for quick indexing
    df_subject = df_subject.set_index('BID')
    df_subject['RACE_STR'] = df_subject['RACE'].map(race_map)
    df_subject['ETHNIC_STR'] = df_subject['ETHNIC'].map(ethnicity_map)
    df_subject['SEX_STR'] = df_subject['SEX'].map(gender_map)


    subject_visit_sheet = in_dir / 'SV.csv'
    df_visits = pd.read_csv(subject_visit_sheet,
                            dtype = {'VISITCD': 'str'})
    df_visits = df_visits.set_index(['BID','VISITCD'])

    cdr_sheet = in_dir / 'cdr.csv'
    df_cdr = pd.read_csv(cdr_sheet,
                         dtype = {'VISCODE': 'str'})
    df_cdr = df_cdr.set_index(['BID','VISCODE'])
    df_cdr = df_cdr.loc[:,['CDSOB','CDRSB','CDGLOBAL']]

    mmse_sheet = in_dir / 'mmse.csv'
    df_mmse = pd.read_csv(mmse_sheet,
                          dtype={'VISCODE': 'str'})
    df_mmse = df_mmse.set_index(['BID','VISCODE'])
    df_mmse = df_mmse.loc[:,['MMSCORE']]

    a4_scans = in_dir.glob('*.json')
    with xnat.connect(xnat_host) as xnat_session:
        xnat_project = xnat_session.projects[notepad_project]

        for json_path in sorted(a4_scans):
            if i < start_i:
                i=i+1
                continue
            print(f"{i} - {json_path.name}")
            json_name = str(json_path)
            # Check to see if there is both a JSON and a GZIPPED NII        
            nii_name = json_name.replace('.json','.nii.gz')
            nii_path = Path(nii_name)
            if not nii_path.exists():
                print('This is not a complete set, the nifi file is missing')
                continue
            # The file names look pretty sensible, delineated by _
            # First split: GROUP (A4, LEARN, SF)
            # Secont split: modality (PET/MR)
            # Third split: Submodality (T1 for MR, tracer for PET)
            # Fourth split: Subject ID
            # Fifith split: Visit Code
            image_parts = json_path.stem.split('_')
            subject_group = image_parts[0]
            modality = image_parts[1]
            submodality = image_parts[2]
            subject_id = image_parts[3]
            # Think about what imaging custom variables you want here. 
            visit_id = image_parts[4]
            print(f"Subject ID: {subject_id}")
            print(f"Study Group: {subject_group}")
            print(f"Visit ID: {visit_id}")
            print(f"Modality: {modality}")
            print(f"Sequence/Tracer: {submodality}")
            if (subject_id,visit_id) in df_visits.index:
                visit_info = df_visits.loc[(subject_id,visit_id)]
                print(visit_info)
                visit_label = visit_info['VISIT']
                days_to_random = visit_info['SVSTDTC_DAYS_T0']
            else:
                print('Error visit info not found for:')
                print(subject_id)
                print(visit_id)
                continue 

            cdr_sob = '-1'
            cdr_global = 'NA'
            mmse = '-1'
            if (subject_id,visit_id) in df_cdr.index:
                cdr_info = df_cdr.loc[(subject_id,visit_id)]
                cdr_sob = cdr_info['CDSOB']
                cdr_global = cdr_info['CDGLOBAL']
            if (subject_id,visit_id) in df_mmse.index:
                mmse_info = df_mmse.loc[(subject_id,visit_id)]
                mmse = mmse_info['MMSCORE']
            
            if modality=="PET":
                radiopharm = submodality.replace("FBP","AV45")
                radiopharm = submodality.replace("FTP","AV1451")
                experiment_id = f"{subject_id}-{visit_id}-{modality}-{radiopharm}"
            else:
                experiment_id = f"{subject_id}-{visit_id}-{modality}"
                
            xnat_subject = create_subject(xnat_session,
                                          xnat_project,
                                          subject_id,
                                          df_subject)
            if xnat_subject is not None:
                experiment = create_experiment(xnat_session,
                                               xnat_subject,
                                               modality,
                                               experiment_id,
                                               nii_path,
                                               json_path,
                                               visit_label,
                                               days_to_random,
                                               cdr_sob,
                                               cdr_global,
                                               mmse)
            if i >= max_i and max_i > 0:
                print("Hit stopping condition")
                sys.exit(1)
            else:
                i=i+1

        
if __name__ == "__main__":
    main()
