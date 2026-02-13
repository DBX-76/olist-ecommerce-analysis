"""
Cross-platform runner for the full pipeline.

Usage:
  python run_pipeline.py

This script:
- creates a local virtual environment in `.venv` if missing
- installs `requirements.txt` into the venv
- runs the pipeline steps in order and stops on first error

Designed to be runnable on Windows/macOS/Linux without PowerShell.
"""
from pathlib import Path
import sys
import subprocess
import venv
import os

ROOT = Path(__file__).parent.resolve()
VENV_DIR = ROOT / ".venv"

def create_venv():
    if not VENV_DIR.exists():
        print("Creating virtual environment at .venv...")
        venv.create(VENV_DIR, with_pip=True)
    py = VENV_DIR / ("Scripts" if os.name == 'nt' else "bin") / ("python.exe" if os.name == 'nt' else "python")
    if not py.exists():
        raise RuntimeError(f"Python executable not found in venv at {py}")
    return str(py)

def run(cmd, env=None):
    print(f"\n>>> Running: {' '.join(cmd)}")
    completed = subprocess.run(cmd, cwd=ROOT, env=env)
    if completed.returncode != 0:
        raise SystemExit(completed.returncode)

def main():
    py = create_venv()

    # Install requirements
    print("Installing requirements...")
    run([py, "-m", "pip", "install", "-r", "requirements.txt"])

    steps = [
        "scripts/analysis/analyze_data_quality.py",
        "scripts/analysis/clean_data.py",
        "scripts/transform_csv_dataset/create_zip_code_reference.py",
        "scripts/transform_csv_dataset/standardize_customers.py",
        "scripts/transform_csv_dataset/enrich_customers_with_geolocation.py",
        "scripts/transform_csv_dataset/detect_seller_anomalies.py",
        "scripts/transform_csv_dataset/standardize_sellers.py",
        "scripts/transform_csv_dataset/enrich_sellers_with_geolocation.py",
        "scripts/transform_csv_dataset/merge_product_translations.py",
        "scripts/transform_csv_dataset/advanced_financial_cleaning.py",
        "scripts/db/init_db.py",       
        "scripts/db/load_data.py",     
        "scripts/analysis/analyze_data_quality.py",
        "test_pipeline.py",
    ]

    for step in steps:
        script_path = ROOT / step
        if not script_path.exists():
            print(f"Skipping missing script: {script_path}")
            continue
        run([py, str(script_path)])

    print("\nPipeline completed successfully.")

if __name__ == '__main__':
    try:
        main()
    except SystemExit as e:
        code = int(e.code) if e.code is not None else 1
        print(f"Pipeline failed with exit code {code}")
        sys.exit(code)
    except Exception as exc:
        print(f"Error running pipeline: {exc}")
        sys.exit(1)
