import pandas as pd
import numpy as np
import os
import time

# Constants for processed files records
PROCESSED_FILES_RECORD_TILT = 'processed_files_tilt.txt'
PROCESSED_FILES_RECORD_CRACK = 'processed_files_crack.txt'
MERGE_STATUS_RECORD = 'merge_status.txt'


def check_merge_status():
    if os.path.exists(MERGE_STATUS_RECORD):
        with open(MERGE_STATUS_RECORD, 'r') as file:
            status = file.read().strip()
        return status == "DONE"
    return False


def update_merge_status():
    with open(MERGE_STATUS_RECORD, 'w') as file:
        file.write("DONE")


def append_new_csv_files(input_folder, output_file, processed_files_record, patterns):
    if os.path.exists(processed_files_record):
        with open(processed_files_record, 'r') as file:
            processed_files = set(file.read().splitlines())
    else:
        processed_files = set()

    new_data_list = []
    new_processed_files = []

    for file_name in os.listdir(input_folder):
        if (file_name.endswith('.csv') and 
            file_name not in processed_files and 
            any(pattern in file_name for pattern in patterns)):
            file_path = os.path.join(input_folder, file_name)
            data = pd.read_csv(file_path)
            data.replace(0, np.nan, inplace=True)
            data.dropna(how='all', inplace=True)
            if not data.empty:
                new_data_list.append(data)
                new_processed_files.append(file_name)
                print(f"Processed new file: {file_name}")

    if not new_data_list:
        return

    new_data = pd.concat(new_data_list, ignore_index=True)
    combined_data = new_data

    if os.path.exists(output_file):
        existing_data = pd.read_csv(output_file)
        combined_data = pd.concat([existing_data, new_data], ignore_index=True)

    combined_data.replace(0, np.nan, inplace=True)
    combined_data.dropna(how='all', inplace=True)

    if 'Date Time (UTC+08:00)' in combined_data.columns:
        combined_data = combined_data.groupby('Date Time (UTC+08:00)', as_index=False).sum()
    else:
        print(f"Column 'Date Time (UTC+08:00)' not found in {output_file}")

    combined_data.to_csv(output_file, index=False)
    print(f"Updated merged data in {output_file}")

    with open(processed_files_record, 'a') as f:
        f.write("\n".join(new_processed_files) + "\n")


def merge_csv_files(input_folder, output_file, patterns, processed_files_record):
    all_data = []
    processed_files = []

    for file_name in os.listdir(input_folder):
        if file_name.endswith('.csv') and any(p in file_name for p in patterns):
            file_path = os.path.join(input_folder, file_name)
            data = pd.read_csv(file_path)
            data.replace(0, np.nan, inplace=True)
            data.dropna(how='all', inplace=True)
            if not data.empty:
                all_data.append(data)
                processed_files.append(file_name)
                print(f"Merged file: {file_name}")

    if not all_data:
        print("No data found for merging.")
        return

    merged_data = pd.concat(all_data, ignore_index=True)

    if 'Date Time (UTC+08:00)' in merged_data.columns:
        merged_data = merged_data.groupby('Date Time (UTC+08:00)', as_index=False).sum()
    else:
        print(f"Column 'Date Time (UTC+08:00)' not found in merged data for {output_file}")

    merged_data.to_csv(output_file, index=False)
    print(f"Successfully created merged file: {output_file}")

    with open(processed_files_record, 'w') as f:
        f.write("\n".join(processed_files) + "\n")


def run_periodic_append_and_merge(input_folder_tilt, output_file_tilt, input_folder_crack, output_file_crack, interval_minutes):
    if not check_merge_status():
        print("Performing initial merge for tilt meters...")
        merge_csv_files(input_folder_tilt, output_file_tilt, ['DG1', 'DG2', 'DG3'], PROCESSED_FILES_RECORD_TILT)
        print("Performing initial merge for crack meters...")
        merge_csv_files(input_folder_crack, output_file_crack, ['CM01', 'CM02', 'CM03', 'CM04'], PROCESSED_FILES_RECORD_CRACK)
        update_merge_status()
    else:
        print("Initial merge already completed. Switching to append mode.")

    while True:
        print("Checking for new tilt meter files...")
        append_new_csv_files(input_folder_tilt, output_file_tilt, PROCESSED_FILES_RECORD_TILT, ['DG1', 'DG2', 'DG3'])
        print("Checking for new crack meter files...")
        append_new_csv_files(input_folder_crack, output_file_crack, PROCESSED_FILES_RECORD_CRACK, ['CM01', 'CM02', 'CM03', 'CM04'])
        print(f"Waiting for {interval_minutes} minutes before next check...")
        time.sleep(interval_minutes * 60)


#Configuration
input_folder_tilt = r"C:\Users\workstationgeo"
output_file_tilt = r"F:\Manikaran project\Manikaran_rockfall\LEWS-Data\GUI_app\code-new\Mohd\PREPROCESSING__DASH\INPUT\tilt_meter_merged_data_MANIKARAN.csv"
input_folder_crack = r"C:\Users\workstationgeo"
output_file_crack = r"F:\Manikaran project\Manikaran_rockfall\LEWS-Data\GUI_app\code-new\Mohd\PREPROCESSING__DASH\INPUT\crack_meter_merged_data_MANIKARAN.csv"
interval_minutes = 120 # Set the interval for periodic append in minutes

# Run the periodic append and merge process
run_periodic_append_and_merge(input_folder_tilt, output_file_tilt, input_folder_crack, output_file_crack, interval_minutes)