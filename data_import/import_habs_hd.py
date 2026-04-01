#!/usr/bin/env python3
"""
import_xnat_data.py

Imports imaging data into XNAT from a structured directory tree.

Directory structure expected:
    <images_dir>/
        <XXXX>/                          # 4-digit subject code  (level 1)
            <series_name>/               # ignored – read from DICOM          (level 2)
                <YYYY-MM-DD_HH_MM_SS.S>/ # scan datetime                     (level 3)
                    *.dcm                # DICOM files                        (level 4)

After successful upload, each level-1 subject folder is moved to:
    <images_dir>/uploaded/<XXXX>/

Demographics are read from:
    <spreadsheets_dir>/RP_HD_7_Clinical.csv

Usage:
    python import_xnat_data.py \\
        --server  https://xnat.example.com \\
        --project MY_PROJECT \\
        --spreadsheets /path/to/spreadsheets \\
        --images   /path/to/images

Dependencies:
    pip install xnat pydicom pandas
"""

import argparse
import logging
import shutil
import sys
import tempfile
import zipfile
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import pandas as pd
import pydicom
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
# Constants
# ---------------------------------------------------------------------------

SUBJECT_PREFIX = "HABSHD_"
DEMOGRAPHICS_FILE = "RP_HD_7_Clinical.csv"
DATETIME_FMT = "%Y-%m-%d_%H_%M_%S.%f"   # e.g. 2023-04-15_10_30_00.0
DATETIME_FMT_ALT = "%Y-%m-%d_%H_%M_%S"  # fallback without fractional seconds
DATE_FMT_SESSION = "%Y%m%d"
gender_map = {
        '0' : 'male',
        '1' : 'female'
    }

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Import structured DICOM data into an XNAT project."
    )
    parser.add_argument("--server",      required=True, help="XNAT server base URL.")
    parser.add_argument("--project",     required=True, help="XNAT project ID.")
    parser.add_argument("--spreadsheets",required=True, help="Directory containing CSV spreadsheets.")
    parser.add_argument("--images",      required=True, help="Root directory of imaging data.")
    parser.add_argument("--dry-run",     action="store_true",
                        help="Validate and log actions without uploading or moving files.")
    parser.add_argument("--verbose","-v",action="store_true", help="Debug-level logging.")
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Demographics helpers
# ---------------------------------------------------------------------------

def load_demographics(spreadsheets_dir: Path) -> pd.DataFrame:
    csv_path = spreadsheets_dir / DEMOGRAPHICS_FILE
    if not csv_path.exists():
        log.error("Demographics file not found: %s", csv_path)
        sys.exit(1)
    df = pd.read_csv(csv_path, dtype=str)
    df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
    df['gender_str'] = df['ID_Gender'].map(gender_map)
    df = df.sort_values(by=["Med_ID","Visit_ID"])
    log.info("Loaded demographics: %d rows from %s", len(df), csv_path)
    return df


def get_demographics(df: pd.DataFrame, four_digit_code: str) -> dict:
    """Return a dict of XNAT demographic fields for the given Med_ID code."""
    matches = df[df["Med_ID"] == four_digit_code]
    if matches.empty:
        log.warning("No demographics row found for Med_ID='%s'.", four_digit_code)
        return {}
    if len(matches) > 1:
        log.warning("Multiple demographics rows for Med_ID='%s'; using first.", four_digit_code)
    row = matches.iloc[0]

    def get(col):
        val = row.get(col, None)
        return None if pd.isna(val) else str(val)
    demographics = {
        "age":       float(get("Age")),
        "race":      get("Ethnicity"),
        "gender":    get("gender_str"),
        "education": int(get("ID_Education")),
    }
    log.debug(f"Retrieved demographics for {four_digit_code}: {demographics}")
    return demographics


# ---------------------------------------------------------------------------
# DICOM helpers
# ---------------------------------------------------------------------------

def read_dicom_header(dcm_path: Path) -> pydicom.Dataset | None:
    """Read only the DICOM header (no pixel data)."""
    try:
        return pydicom.dcmread(str(dcm_path), stop_before_pixels=True)
    except Exception as exc:
        log.warning("Cannot read DICOM header from %s: %s", dcm_path, exc)
        return None


def get_modality(dcm_files: list[Path]) -> str:
    """Return 'MR', 'PT' (→ 'PET'), or 'UNKNOWN' from the first readable DICOM."""
    for f in dcm_files:
        ds = read_dicom_header(f)
        if ds is None:
            continue
        modality = getattr(ds, "Modality", "").upper()
        if modality == "PT":
            return "PET"
        if modality:
            return modality
    return "UNKNOWN"


def parse_scan_datetime(dirname: str) -> datetime | None:
    """Parse YYYY-MM-DD_HH_MM_SS.S or YYYY-MM-DD_HH_MM_SS into a datetime."""
    for fmt in (DATETIME_FMT, DATETIME_FMT_ALT):
        try:
            return datetime.strptime(dirname, fmt)
        except ValueError:
            continue
    return None


# ---------------------------------------------------------------------------
# Directory discovery
# ---------------------------------------------------------------------------

def discover_subjects(images_dir: Path) -> list[Path]:
    """Return all 4-digit subdirectories under images_dir, skipping 'uploaded'."""
    subjects = []
    for d in sorted(images_dir.iterdir()):
        if not d.is_dir():
            continue
        if d.name == "uploaded":
            continue
        if not d.name.isdigit() or len(d.name) != 4:
            log.warning("Skipping unexpected directory (not 4 digits): %s", d.name)
            continue
        subjects.append(d)
    return subjects


def collect_sessions(subject_dir: Path) -> dict[str, list[tuple[Path, datetime]]]:
    """
    Walk a subject directory and group scan-datetime directories by date.

    Returns:
        { 'YYYYMMDD': [(datetime_dir_path, datetime_obj), ...], ... }
    """
    by_date: dict[str, list[tuple[Path, datetime]]] = defaultdict(list)

    for series_dir in sorted(subject_dir.iterdir()):   # level 2 – ignored label
        if not series_dir.is_dir():
            continue
        for dt_dir in sorted(series_dir.iterdir()):    # level 3 – datetime
            if not dt_dir.is_dir():
                continue
            dt_obj = parse_scan_datetime(dt_dir.name)
            if dt_obj is None:
                log.warning("Cannot parse datetime directory name: %s — skipping.", dt_dir)
                continue
            date_key = dt_obj.strftime(DATE_FMT_SESSION)
            by_date[date_key].append((dt_dir, dt_obj))

    return by_date


# ---------------------------------------------------------------------------
# XNAT operations
# ---------------------------------------------------------------------------

def ensure_subject(xnat_session,
                   xnat_project, 
                   subject_label: str, demographics: dict, dry_run: bool):
    """Create an XNAT subject if it does not already exist, and set demographics."""
    
    subject = None
    if subject_label in xnat_project.subjects:
        log.info("Subject already exists: %s", subject_label)
        subject = xnat_project.subjects[subject_label]
    else:
        log.info("Creating subject: %s", subject_label)
        if dry_run:
            log.info("[DRY-RUN] Would create subject %s with demographics %s",
                    subject_label, demographics)
            return None

        subject = xnat_session.classes.SubjectData(
                    parent=xnat_project, 
                    label=subject_label)

    # Map demographics into XNAT's xnat:subjectData fields
    field_map = {
        "age":       ("age",       None),
        "gender":    ("gender",    None),
        "race":      ("race",      None),
        "education": ("education", None),
    }

#    subject.demographics.age = field_map["age"]
#    subject.demographics.gender = field_map["gender"]
#    subject.demographics.education = field_map["education"]
#    subject.demographics.race = field_map["race"]

    for key, (attr, _) in field_map.items():
        value = demographics.get(key)
        if value:
            try:
                setattr(subject.demographics, attr, value)
            except Exception:
                # Fall back to custom fields if the standard attribute is unavailable
                subject.fields[key] = value

    log.info("Subject created and saved: %s", subject_label)
    return subject


def ensure_session(xnat_session,xnat_subject, session_label: str, modality: str, date_str: str,
                   dry_run: bool):
    """Create an XNAT imaging session (experiment) if it does not exist."""
    if session_label in {e.label for e in xnat_subject.experiments.values()}:
        log.info("Session already exists: %s", session_label)
        for e in xnat_subject.experiments.values():
            if e.label == session_label:
                return e
        return None

    log.info("Creating session: %s  (modality=%s, date=%s)", session_label, modality, date_str)
    if dry_run:
        log.info("[DRY-RUN] Would create session %s", session_label)
        return None

    if modality == "MR":
        xnat_experiment = xnat_session.classes.MrSessionData(
                parent=xnat_subject, label=session_label)
    elif modality == "PET":
        xnat_experiment = xnat_session.classes.PetSessionData(
                parent=xnat_subject, label=session_label)
    else:
        log.warning(f"Modality {modality} is unknown. Not creating session {session_label}")
        return None

    try:
        xnat_experiment.date = date_str  # YYYYMMDD
    except Exception:
        pass
    log.info("Session created: %s", session_label)
    return xnat_experiment

def upload_dicom_files(xnat_session, 
                       project_id: str,
                       subject_label:str,
                       session_label: str,
                       dt_dirs: list[tuple[Path, datetime]],
                       dry_run: bool) -> bool:
    """
    Zip all DICOM files from the given datetime directories and upload
    the archive to the XNAT session via the import service.
    """
    all_dcm: list[Path] = []
    for dt_dir, _ in dt_dirs:
        all_dcm.extend(sorted(dt_dir.glob("**/*.dcm")))

    if not all_dcm:
        log.warning("No .dcm files found for session %s — skipping.", session_label)
        return True

    log.info("  Zipping %d DICOM file(s) for session %s", len(all_dcm), session_label)

    if dry_run:
        log.info("[DRY-RUN] Would zip and upload %d file(s)", len(all_dcm))
        return True
    
    tmp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
            tmp_path = Path(tmp.name)

        with zipfile.ZipFile(tmp_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for dcm_file in all_dcm:
                zf.write(dcm_file, arcname=dcm_file.name)

        log.info("  Uploading zip to session %s", session_label)
        xnat_session.services.import_(tmp_path, 
                             overwrite="none",
                             project=project_id,
                             subject=subject_label,
                             experiment=session_label,
                             content_type="application/zip")
        log.info("  Upload complete for session %s", session_label)
        return True

    except Exception as exc:
        log.error("  Upload failed for session %s: %s", session_label, exc)
        return False

    finally:
        if tmp_path is not None and tmp_path.exists():
            tmp_path.unlink()

def move_to_uploaded(subject_dir: Path, uploaded_dir: Path, dry_run: bool) -> None:
    """Move a subject folder into <images_dir>/uploaded/ on successful upload."""
    dest = uploaded_dir / subject_dir.name
    if dry_run:
        log.info("[DRY-RUN] Would move %s → %s", subject_dir, dest)
        return
    if dest.exists():
        log.warning("Destination already exists, removing: %s", dest)
        shutil.rmtree(dest)
    shutil.move(str(subject_dir), str(dest))
    log.info("Moved %s → %s", subject_dir.name, dest)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    images_dir      = Path(args.images).resolve()
    spreadsheets_dir = Path(args.spreadsheets).resolve()

    if not images_dir.is_dir():
        log.error("Images directory not found: %s", images_dir)
        sys.exit(1)
    if not spreadsheets_dir.is_dir():
        log.error("Spreadsheets directory not found: %s", spreadsheets_dir)
        sys.exit(1)

    # Prepare the 'uploaded' staging directory
    uploaded_dir = images_dir / "uploaded"
    if not args.dry_run:
        uploaded_dir.mkdir(exist_ok=True)

    # Load demographics
    demographics_df = load_demographics(spreadsheets_dir)

    # Discover subject directories
    subject_dirs = discover_subjects(images_dir)
    if not subject_dirs:
        log.info("No 4-digit subject directories found under %s.", images_dir)
        sys.exit(0)

    log.info("Found %d subject director(ies) to process.", len(subject_dirs))

    # Connect to XNAT
    log.info("Connecting to XNAT at %s …", args.server)
    try:
        with xnat.connect(args.server) as xnat_session:
            log.info("Connected.")

            try:
                project = xnat_session.projects[args.project]
            except KeyError:
                log.error("Project '%s' not found on %s.", args.project, args.server)
                sys.exit(1)

            for subject_dir in subject_dirs:
                four_digit = subject_dir.name
                subject_label = f"{SUBJECT_PREFIX}{four_digit}"
                log.info("=" * 60)
                log.info("Processing subject directory: %s  →  %s",
                         four_digit, subject_label)

                # --- Demographics ---
                demog = get_demographics(demographics_df, four_digit)

                # --- Ensure subject exists ---
                xnat_subject = ensure_subject(xnat_session,
                                              project, 
                                              subject_label, demog, args.dry_run)

                # --- Collect and group sessions by date ---
                sessions_by_date = collect_sessions(subject_dir)
                if not sessions_by_date:
                    log.warning("No valid scan directories found under %s.", subject_dir)
                    continue

                subject_upload_ok = True

                for date_key, dt_dirs in sorted(sessions_by_date.items()):
                    # Determine modality from first DICOM in any of the datetime dirs
                    all_dcm: list[Path] = []
                    for dt_dir, _ in dt_dirs:
                        all_dcm.extend(sorted(dt_dir.glob("**/*.dcm")))
                    log.info(f"{date_key}: Found {len(all_dcm)} DICOM files for session ")

                    if not all_dcm:
                        log.warning("No DICOM files found. Skipping")
                        subject_upload_ok = False
                        continue

                    modality = get_modality(all_dcm)
                    if modality == "UNKNOWN":
                        log.warning("Modality could not be determined. Skipping")
                        subject_upload_ok = False
                        continue
                    session_label = f"{subject_label}_{modality}_{date_key}"

                    log.info("  Session: %s  (%d scan director(ies))",
                             session_label, len(dt_dirs))

                    # --- Ensure session exists ---
                    if not args.dry_run and xnat_subject is not None:
                        xnat_experiment = ensure_session(
                            xnat_session,xnat_subject, session_label, modality, date_key, args.dry_run
                        )
                    else:
                        xnat_experiment = None
                        if args.dry_run:
                            log.info("[DRY-RUN] Would create/use session %s", session_label)

                    # --- Upload DICOM files ---
                    if args.dry_run or xnat_experiment is not None:
                        ok = upload_dicom_files(
                            xnat_session,
                            args.project,
                            subject_label,
                            session_label, 
                            dt_dirs, args.dry_run
                        )
                        if not ok:
                            subject_upload_ok = False

                # --- Move subject folder if all sessions uploaded OK ---
                if subject_upload_ok:
                    move_to_uploaded(subject_dir, uploaded_dir, args.dry_run)
                else:
                    log.warning(
                        "One or more uploads failed for subject %s; "
                        "folder NOT moved to uploaded/.", subject_label
                    )

    except Exception as exc:
        log.error("Fatal error: %s", exc, exc_info=args.verbose)
        sys.exit(1)

    log.info("Import complete.")


if __name__ == "__main__":
    main()