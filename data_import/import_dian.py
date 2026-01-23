import xnat
import pandas as pd
import argparse
from pathlib import Path

# Add argparse to provide MR and PET session data freeze CSV lists
# So that we don't have to get them again

#URL for NOTEPAD local server 
notepad_uri = "https://xnat-srv.drc.ion.ucl.ac.uk"
# Project for data
notepad_project = "NOTEPAD_DIAN"

#URL for CNDA
cnda_uri = "https://cnda.wustl.edu"
# Project for data
cnda_project = "DIANDF17"

def get_session_list(xnat_host,project,modality):
    df_sessions={}
    with xnat.connect(xnat_host,
                    extension_types=False,
                    loglevel="ERROR") as xnat_server:
        experiments_uri = f"/REST/projects/{project}/experiments"
        xsi_type = f"xnat:{modality}SessionData"
        sessions_query = {
            "xsiType": xsi_type,
            "columns": "subject_label,label,date,time"
        }
        print(f"Getting sessions of {xsi_type} from {xnat_host}") 
        response_json = xnat_server.get_json(experiments_uri,
                                            sessions_query)
        if response_json is None:
            print("No results were found")
            sys.exit(1)
        df_sessions = pd.DataFrame(
            sorted(response_json["ResultSet"]["Result"],
                key=lambda k: k["label"])
        )
        if df_sessions.empty:
            print(f"No sessions of {xsi_type} in {xnat_host}")
    return(df_sessions)

def transfer_session(df_transfer):
    for label,session_data in df_transfer.iterrows():
        print(label)
        dl_path=Path(f"/tmp/{label}")
        with xnat.connect(cnda_uri,
                          extension_types=False,
                          loglevel="ERROR") as xnat_source_server:
            experiment_uri = f"/REST/projects/{cnda_project}/experiments/{label}"
            experiment = xnat_source_server.create_object(experiment_uri)
            t1_scans = [x.id for x in experiment.scans if "MPRAGE" in x.type]
            flair_scans = [x.id for x in experiment.scans if "FLAIR" in x.type]
            scans_to_upload = t1_scans + flair_scans
            for scan_id in scans_to_upload:
                experiment.scans[scan_id].download_dir("/tmp")
        with xnat.connect(notepad_uri,
                          extension_types=False,
                          loglevel="ERROR") as xnat_dest_server: 
            dest_project = xnat_dest_server.projects[notepad_project]
            dest_subjects = dest_project.subjects
            if session_data.subject_label not in dest_subjects:
                xnat_dest_subject = xnat_dest_server.classes.SubjectData(
                parent=dest_project, 
                label=session_data.subject_label)
            archive_session = xnat_dest_server.services.import_dir(
                                dl_path, 
                                project=dest_project, 
                                subject=xnat_dest_subject,
                                experiment=label)
            

def main():
    parser = argparse.ArgumentParser(
        description='Import DIAN CNDA DICOM to NOTEPAD XNAT')
    help_str = """
    Path to store file of MR sessions on CNDA. 
    If it does not exist it will be created to this location. 
    """
    parser.add_argument('--mr_sessions', 
                        type=str,
                        required=True,
                        help=help_str)
    help_str = """
    Path to store file of PET sessions on CNDA. 
    If it does not exist it will be created to this location. 
    """
    parser.add_argument('--pet_sessions', 
                        type=str,
                        required=True,
                        help=help_str)
    args = parser.parse_args()

    modality_list = []
    mrsession_list_path = Path(args.mr_sessions)
    if not mrsession_list_path.exists():
        df_mrsessions = get_session_list(cnda_uri,
                                         cnda_project,
                                         "mr")
        df_mrsessions.to_csv(mrsession_list_path)
    else:
        df_mrsessions = pd.read_csv(mrsession_list_path)

    petsession_list_path = Path(args.pet_sessions)
    if not petsession_list_path.exists():
        df_petsessions = get_session_list(cnda_uri,
                                          cnda_project,
                                          "pet")
        df_petsessions.to_csv(petsession_list_path)
    else:
        df_petsessions = pd.read_csv(petsession_list_path)

    # Now do same thing for local XNAT
    # This one I don't want preloaded, as it will be dynamic
    # Based on the upload
    df_mruploaded = get_session_list(notepad_uri,
                                     notepad_project,
                                     "mr")
    if df_mruploaded.empty:
        df_toupload = df_mrsessions
    else:
        df_toupload = pd.merge(df_mruploaded,
                            df_mrsessions,
                            how="right",
                            suffixes=['_local',''],
                            on=["subjectlabel,label"],
                            indicator = True
                            )
        df_toupload = df_toupload.loc[df_toupload["_merge"]=="right_only"]

    df_toupload = df_toupload.set_index('label')
    print(len(df_toupload))
    transfer_session(df_toupload)
    df_petuploaded = get_session_list(notepad_uri,
                                     notepad_project,
                                     "pet")
    if df_petuploaded.empty:
        df_toupload = df_petsessions
    else:
        df_toupload = pd.merge(df_petuploaded,
                            df_petsessions,
                            how="right",
                            suffixes=['_local',''],
                            on=["subjectlabel,label"],
                            indicator = True
                            )
        df_toupload = df_toupload.loc[df_toupload["_merge"]=="right_only"]
    df_toupload = df_toupload.set_index('label')
    print(len(df_toupload))
    transfer_session(df_toupload)


if __name__ == "__main__":
    main()
