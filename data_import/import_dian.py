import xnat
import pandas as pd

# Add argparse to provide MR and PET session data freeze CSV lists
# So that we don't have to get them again

#URL for NOTEPAD local server 
notepad_xnat_uri = "https://xnat-srv.drc.ion.ucl.ac.uk"
# Project for data
notepad_project = "NOTEPAD_DIAN"

#URL for CNDA
cnda_uri = "https://cnda.wustl.edu"
# Project for data
cnda_project = "DIANDF17"


with xnat.connect(cnda_uri,
                  extension_types=False) as xnat_cnda:
    cnda_experiments_uri = f"/REST/projects/{cnda_project}/experiments"
    xsi_type_list = [
        "xnat:mrSessionData",
         "xnat:petSessionData"
        ]
    for xsi_type in xsi_type_list:
        sessions_query = {
            "xsi_type": xsi_type,
            "columns": "subject_label,label,date,time"
        }
        print(f"Getting sessions of {xsi_type}") 
        response_json = xnat_cnda.get_json(cnda_experiments_uri,
                                            sessions_query)
        if response_json is None:
            print("No results were found")
            sys.exit(1)
        df_sessions = pd.DataFrame(
            sorted(response_json["ResultSet"]["Result"],
                   key=lambda k: k["label"])
        )
        df_sessions = df_sessions.set_index('label')
        print(df_sessions)
    
    petsessions_query = {
        "xsi_type": "xnat:petSessionData", 
        "columns": "subject_label,label,date,time"
    }

