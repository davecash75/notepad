import sys
from pathlib import Path
import pandas as pd
import xnat 
import re
import time
import shutil
import argparse

def load_spreadsheet(path: str) -> pd.DataFrame:
    """Load a spreadsheet into a DataFrame, supporting CSV, TSV, and Excel."""
    p = Path(path)
    if not p.exists():
        print(f"Spreadsheet not found: {path}")
        sys.exit(1)

    suffix = p.suffix.lower()
    print(f"Loading spreadsheet: {path}")

    if suffix in (".csv",):
        df = pd.read_csv(path, dtype=str)
    elif suffix in (".tsv",):
        df = pd.read_csv(path, sep="\t", dtype=str)
    elif suffix in (".xlsx", ".xlsm", ".xls", ".ods"):
        df = pd.read_excel(path, dtype=str)
    else:
        print(
            f"Unrecognised extension {suffix}. Attempting CSV read."
            )
        df = pd.read_csv(path, dtype=str)

    # Strip leading/trailing whitespace from all string values
    df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
    n_rows = len(df)
    n_cols = len(df.columns)
    print(f"Loaded {n_rows} rows and {n_cols} columns.")
    return df


# Some helpful globals
# Host for the xnat where data is going
xnat_host = "https://xnat-srv.drc.ion.ucl.ac.uk"
# Project for data
notepad_project = "NOTEPAD_ADNI"


def main():

    parser = argparse.ArgumentParser(
        description='Reconcile ADNI data with NOTEPAD XNAT')
    
    parser.add_argument(
        "--spreadsheet", "-s",
        required=True,
        help="Path to the input spreadsheet (.csv, .xlsx, .xls, .tsv).",
    )
    parser.add_argument(
        "--registry", "-r",
        type=str,
        required=True,
        help='Location of registry spreadsheet to reconcile viscodes')
    
    parser.add_argument(
        "--output", "-o",
        required=True,
        help="Path for the output report (written as CSV).",
    )
    parser.add_argument(
        "--modality","-m",
        required=True,
        choices=("MR","PET"),
        help="Choose modality: MR or PET"
    )
    args = parser.parse_args()
    df_manifest = load_spreadsheet(args.spreadsheet)

    df_registry = load_spreadsheet(args.registry)
    df_registry = df_registry[
        ["PTID","RID","VISCODE","VISCODE2","EXAMDATE"]
        ]
    df_registry = df_registry.rename(
        columns={'PTID':'subject_id',
                 'VISCODE':'image_visit',
                 'VISCODE2':'visit'})

    df_out = pd.merge(df_manifest,df_registry,
                      on=['subject_id','image_visit'],
                      how='left')
    
    if args.modality == "MR":
        df_out['session_label'] = df_out["subject_id"] + "-" + df_out["visit"] + "-" + args.modality
    else:
        df_out['tracer'] = df_out["image_description"].str.split().str[0]
        df_out['session_label'] = df_out["subject_id"] + "-" + df_out["visit"] + "-" + args.modality + df_out["tracer"] 
    df_out.to_csv(args.output)

    
if __name__ == "__main__":
    main()
