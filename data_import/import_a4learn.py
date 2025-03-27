import argparse
from pathlib import Path

import xnat

# Some helpful globals
# Host for the xnat where data is going
xnat_host = "https://xnat-srv.drc.ion.ucl.ac.uk"
# Project for data
notepad_project = "NOTEPAD_A4LEARN"

def upload_files(session, project, subject, experiment, experiment_type, scan, scan_description, resource, data):
    if experiment_type not in ["PT", "MR"]:
        print(f"[ERROR] experiment type {experiment_type} not supported use 'MR' or 'CT'")
        return

    xnat_project = session.projects[project]
    # If subject does not exist create subject
    if subject in xnat_project.subjects:
        xnat_subject = xnat_project.subjects[subject]
    else:
        xnat_subject = session.classes.SubjectData(parent=xnat_project, label=subject)

    # if experiment does not create new experiment
    if experiment in xnat_subject.experiments:
        xnat_experiment = xnat_subject.experiments[experiment]
    else:
        if experiment_type == "CT":
            xnat_experiment = session.classes.CtSessionData(parent=xnat_subject, label=experiment)
        elif experiment_type == "MR":
            xnat_experiment = session.classes.MrSessionData(parent=xnat_subject, label=experiment)
        else:
            print(f"[ERROR] experiment type {experiment_type} not supported use 'MR' or 'CT'")
            return

    # if scan does not exist create new Scan
    if scan in xnat_experiment.scans:
        xnat_scan = xnat_experiment.scans[scan]
    else:
        if experiment_type == "CT":
            xnat_scan = session.classes.CtScanData(
                parent=xnat_experiment, id=scan, type=scan, series_description=scan_description, label=scan
            )
        elif experiment_type == "MR":
            xnat_scan = session.classes.MrScanData(
                parent=xnat_experiment, id=scan, type=scan, series_description=scan_description, label=scan
            )
        else:
            print(f"[ERROR] scan type {experiment_type} not supported use 'MR' or 'CT'")
            return

    # If resource exists create new resource
    if resource in xnat_scan.resources:
        xnat_resource = xnat_scan.resources[resource]
    else:
        xnat_resource = session.classes.ResourceCatalog(parent=xnat_scan, label=resource)
    for file_ in data:
        file_ = Path(file_)
        if file_.exists():
            xnat_resource.upload(str(file_), file_.name)
        else:
            print(f"[WARNING] Could not find file: {file_}")
    pass


def main():
    parser = argparse.ArgumentParser(
            description='Import ADNI DICOM to NOTEPAD XNAT')
    parser.add_argument('--in_path', type=str,
                    required=True,
                    help='Path to subject to upload')
    parser.add_argument("--experiment", required=True, help="session")
    parser.add_argument("--experiment-type", required=False, help="session type (CT/MR)")
    parser.add_argument("--scan", required=True, help="scan id")
    parser.add_argument("--scan-description", required=False, help="scan description")
    parser.add_argument("--resource-name", required=True, help="resource name")
    parser.add_argument("--files", nargs="+", required=True, help="list of files")
    args = parser.parse_args()

    print("subject: {}".format(args.subject))
    print("experiment: {}".format(args.experiment))
    print("experiment-type: {}".format(args.experiment_type))
    print("scan: {}".format(args.scan))
    print("scan-description: {}".format(args.scan_description))
    print("resource-name: {}".format(args.resource_name))
    print("files:")

    for filename in args.files:
        print("     {}".format(filename))

    with xnat.connect(args.xnathost, user=args.user) as session:
        upload_files(
            session,
            args.project,
            args.subject,
            args.experiment,
            args.experiment_type,
            args.scan,
            args.scan_description,
            args.resource_name,
            args.files,
        )

if __name__ == "__main__":
    main()
