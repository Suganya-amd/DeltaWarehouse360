# Databricks notebook source
import os

# Define source and target paths
volume_path = "/Volumes/etl/bronze/source_data/"
local_data_dir = "../../data"

# List and process each CSV file
for filename in os.listdir(local_data_dir):
    if filename.endswith(".csv"):
        local_file_path = os.path.join(local_data_dir, filename)
        with open(local_file_path, "r") as f:
            csv_content = f.read()

        target_file_path = f"{volume_path}/{filename}"
        dbutils.fs.put(target_file_path, csv_content, True)  # noqa: F821 # type: ignore
        print(f"Deployed {filename} to {target_file_path}")
