#!/usr/bin/env python3
"""
check_xnat_data.py

Validates that imaging data listed in a spreadsheet exists on an XNAT server.
For each row, checks that the subject, session, and scan are present in the
specified XNAT project. Rows with missing data are written to an output report.

Usage:
    python check_xnat_data.py \
        --spreadsheet data.csv \
        --output missing_report.csv \
        --subject-col SubjectID \
        --session-col SessionLabel \
        --series-col SeriesDescription \
        --server https://xnat.example.com \
        --project MY_PROJECT

Dependencies:
    pip install xnat pandas openpyxl
"""

import argparse
import sys
import getpass
import logging
from pathlib import Path

import pandas as pd
import xnat


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Check that imaging data in a spreadsheet exists on XNAT."
    )
    parser.add_argument(
        "--spreadsheet", "-s",
        required=True,
        help="Path to the input spreadsheet (.csv, .xlsx, .xls, .tsv).",
    )
    parser.add_argument(
        "--output", "-o",
        required=True,
        help="Path for the output report (written as CSV).",
    )
    parser.add_argument(
        "--subject-col",
        required=True,
        help="Column name that contains the subject identifier.",
    )
    parser.add_argument(
        "--session-col",
        required=True,
        help="Column name that contains the session/experiment label.",
    )
    parser.add_argument(
        "--series-col",
        required=True,
        help="Column name that contains the scan/series identifier.",
    )
    parser.add_argument(
        "--server",
        required=True,
        help="Base URL of the XNAT server (e.g. https://xnat.example.com).",
    )
    parser.add_argument(
        "--project",
        required=True,
        help="XNAT project ID to search within.",
    )
    parser.add_argument(
        "--sheet",
        default=0,
        help="Sheet name or index for Excel files (default: first sheet).",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable debug-level logging.",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Spreadsheet loading
# ---------------------------------------------------------------------------

def load_spreadsheet(path: str, sheet) -> pd.DataFrame:
    """Load a spreadsheet into a DataFrame, supporting CSV, TSV, and Excel."""
    p = Path(path)
    if not p.exists():
        log.error("Spreadsheet not found: %s", path)
        sys.exit(1)

    suffix = p.suffix.lower()
    log.info("Loading spreadsheet: %s", path)

    if suffix in (".csv",):
        df = pd.read_csv(path, dtype=str)
    elif suffix in (".tsv",):
        df = pd.read_csv(path, sep="\t", dtype=str)
    elif suffix in (".xlsx", ".xlsm", ".xls", ".ods"):
        df = pd.read_excel(path, sheet_name=sheet, dtype=str)
    else:
        log.warning(
            "Unrecognised extension '%s'. Attempting CSV read.", suffix
        )
        df = pd.read_csv(path, dtype=str)

    # Strip leading/trailing whitespace from all string values
    df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
    log.info("Loaded %d rows and %d columns.", len(df), len(df.columns))
    return df


def validate_columns(df: pd.DataFrame, *col_names: str) -> None:
    """Abort early if any required column is missing."""
    missing = [c for c in col_names if c not in df.columns]
    if missing:
        log.error(
            "The following required columns were not found in the spreadsheet: %s\n"
            "Available columns: %s",
            missing,
            list(df.columns),
        )
        sys.exit(1)


# ---------------------------------------------------------------------------
# XNAT helpers
# ---------------------------------------------------------------------------

def get_project(session, project_id: str):
    """Return the XNAT project object, or exit if not found."""
    try:
        return session.projects[project_id]
    except KeyError:
        log.error(
            "Project '%s' not found on %s. "
            "Check the project ID and your permissions.",
            project_id,
            session.url,
        )
        sys.exit(1)


def scan_exists(project, subject_id: str, session_label: str, series_id: str) -> tuple[bool, str]:
    """
    Check whether a subject / session / scan triplet exists in the project.

    Returns:
        (exists: bool, reason: str)
        reason is empty when exists is True, otherwise describes what is missing.
    """
    # --- Subject ---
    if subject_id not in project.subjects:
        return False, f"Subject '{subject_id}' not found"

    subject = project.subjects[subject_id]

    # --- Session / Experiment ---
    # XNAT experiments can be keyed by label or accession; try label first.
    experiment = None
    for exp in subject.experiments.values():
        if exp.label == session_label:
            experiment = exp
            break

    if experiment is None:
        return False, (
            f"Session '{session_label}' not found "
            f"for subject '{subject_id}'"
        )

    # --- Scan / Series ---
    # series_id may be a scan ID (e.g. "4") or a series description.
    # We check both the scan ID and the series_description field.
    scan_found = False
    for scan in experiment.scans.values():
        if scan.id == series_id or getattr(scan, "series_description", None) == series_id:
            scan_found = True
            break

    if not scan_found:
        return False, (
            f"Scan/series '{series_id}' not found "
            f"in session '{session_label}' "
            f"for subject '{subject_id}'"
        )

    return True, ""


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)


    # Load and validate spreadsheet
    df = load_spreadsheet(args.spreadsheet, args.sheet)
    validate_columns(df, args.subject_col, args.session_col, args.series_col)

    # Connect to XNAT
    log.info("Connecting to XNAT at %s …", args.server)
    try:
        with xnat.connect(args.server) as xnat_session:
            log.info("Connected successfully.")
            project = get_project(xnat_session, args.project)

            missing_rows: list[dict] = []
            total = len(df)

            for idx, row in df.iterrows():
                subject_id    = row[args.subject_col]
                session_label = row[args.session_col]
                series_id     = row[args.series_col]

                # Skip rows where any key field is blank / NaN
                if pd.isna(subject_id) or pd.isna(session_label) or pd.isna(series_id):
                    log.warning(
                        "Row %d has a blank key field — skipping "
                        "(subject=%r, session=%r, series=%r).",
                        idx, subject_id, session_label, series_id,
                    )
                    continue

                log.debug(
                    "Checking row %d/%d: subject=%s  session=%s  series=%s",
                    idx + 1, total, subject_id, session_label, series_id,
                )

                exists, reason = scan_exists(
                    project, subject_id, session_label, series_id
                )

                if not exists:
                    log.warning("MISSING — %s", reason)
                    record = row.to_dict()
                    record["_missing_reason"] = reason
                    missing_rows.append(record)
                else:
                    log.debug("OK")

    except Exception as exc:
        log.error("Failed to connect to or query XNAT: %s", exc)
        sys.exit(1)

    # Write report
    n_missing = len(missing_rows)
    log.info(
        "Check complete. %d / %d rows have missing data on XNAT.",
        n_missing,
        total,
    )

    if missing_rows:
        report_df = pd.DataFrame(missing_rows)
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        report_df.to_csv(output_path, index=False)
        log.info("Report written to: %s", output_path)
    else:
        log.info("All rows were found on XNAT. No report written.")


if __name__ == "__main__":
    main()