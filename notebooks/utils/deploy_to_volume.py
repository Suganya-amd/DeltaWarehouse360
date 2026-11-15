# Databricks notebook source
import os

# Define widgets with defaults
dbutils.widgets.text("volume_path", "/Volumes/workspace/default/source_data")
volume_path = dbutils.widgets.get("volume_path").rstrip("/")

# Ensure Unity Catalog volume exists
parts = volume_path.strip("/").split("/")
if len(parts) >= 4 and parts[0] == "Volumes":
    cat, sch, vol = parts[1], parts[2], parts[3]
    try:
        spark.sql(f"CREATE VOLUME IF NOT EXISTS `{cat}`.`{sch}`.`{vol}`")
        print(f"Ensured volume `{cat}`.`{sch}`.`{vol}` exists.")
    except Exception as e:
        print(f"Note on volume check: {e}")

# Determine local data directory robustly across local IDE, Workspace, and cluster environments
base_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()

candidate_dirs = [
    os.path.abspath(os.path.join(base_dir, "../../data")),
    os.path.abspath(os.path.join(base_dir, "../data")),
    os.path.abspath(os.path.join(base_dir, "data")),
    os.path.abspath(os.path.join(base_dir, "DeltaWarehouse360/data")),
    "../../data",
    "./data",
]

local_data_dir = None
for candidate in candidate_dirs:
    if os.path.isdir(candidate):
        local_data_dir = candidate
        break

if not local_data_dir:
    raise FileNotFoundError("Could not find 'data' directory containing CSV files.")

print(f"Reading CSV files from: {local_data_dir}")
print(f"Deploying to volume: {volume_path}")

# List and process each CSV file
deployed_files = []
for filename in sorted(os.listdir(local_data_dir)):
    if filename.endswith(".csv"):
        local_file_path = os.path.join(local_data_dir, filename)
        with open(local_file_path, "r", encoding="utf-8") as f:
            csv_content = f.read()

        target_file_path = f"{volume_path}/{filename}"
        dbutils.fs.put(target_file_path, csv_content, True)  # noqa: F821 # type: ignore
        deployed_files.append(filename)
        print(f"Deployed {filename} to {target_file_path}")

print(f"Successfully deployed {len(deployed_files)} CSV files.")
