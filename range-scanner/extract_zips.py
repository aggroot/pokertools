#!/usr/bin/env python3
import os
import zipfile
from pathlib import Path

def extract_all_zips(root_dir):
    """Recursively extract all zip files in directory"""
    root_path = Path(root_dir)

    while True:
        zip_files = list(root_path.rglob("*.zip"))
        if not zip_files:
            print("All zips extracted!")
            break

        print(f"Found {len(zip_files)} zip files to extract...")

        for zip_path in zip_files:
            try:
                print(f"Extracting: {zip_path}")
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(zip_path.parent)
                os.remove(zip_path)
            except Exception as e:
                print(f"Error extracting {zip_path}: {e}")

if __name__ == "__main__":
    extract_all_zips("hands")
