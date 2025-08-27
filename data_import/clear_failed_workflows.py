import argparse
import pandas as pd
import urllib
import xnat 

parser = argparse.ArgumentParser(
        description='Clear NOTEPAD workflows')

parser.add_argument('insheet', type=str,
                    help='Workflow CSV')

args = parser.parse_args()

xnat_host = "https://xnat-srv.drc.ion.ucl.ac.uk"
# Project for data
df_workflows = pd.read_csv(args.insheet)

with xnat.connect(xnat_host) as xnat_session:
    for idx,workflow in df_workflows.iterrows():
        print(workflow)
        workflow_url = f"/data/workflows/{workflow['Workflow ID']}"
        workflow_query = {
            'wrk:workflowData/status': 'Failed'
        }
        response = xnat_session.put(workflow_url,query=workflow_query)
        print(response)
        workflow_query = {
            'wrk:workflowData/status': 'Failed (Dismissed)'
        }
        response = xnat_session.put(workflow_url,query=workflow_query)
        print(response)