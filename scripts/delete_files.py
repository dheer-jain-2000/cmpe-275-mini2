import os
import shutil
import glob

def delete_files_matching_pattern(directory_path, pattern):
    """Deletes files matching a given pattern in the specified directory."""
    for file_path in glob.glob(os.path.join(directory_path, pattern)):
        try:
            if os.path.isfile(file_path):
                os.remove(file_path)
                print(f'Deleted: {file_path}')
        except Exception as e:
            print(f'Failed to delete {file_path}. Reason: {e}')

def delete_all_files_in_directory(directory_path):
    """Deletes all files and directories in the specified directory."""
    for item in os.listdir(directory_path):
        item_path = os.path.join(directory_path, item)
        try:
            if os.path.isfile(item_path):
                os.remove(item_path)
            elif os.path.isdir(item_path):
                shutil.rmtree(item_path)
            print(f'Deleted: {item_path}')
        except Exception as e:
            print(f'Failed to delete {item_path}. Reason: {e}')

base_path = '/Users/admin/Desktop/CMPE-275-HPC/'  # Adjust your base directory as needed

# Directories to completely clean out
directories_to_clean = ['logs']
for directory in directories_to_clean:
    directory_path = os.path.join(base_path, directory)
    delete_all_files_in_directory(directory_path)

# Delete specific pattern-matched files from the 'data' directory
data_directory = os.path.join(base_path, 'data')
patterns_to_delete = ['cleaned_data_rank_*.csv', 'records_per_second_process_*.csv']
for pattern in patterns_to_delete:
    delete_files_matching_pattern(data_directory, pattern)

print("Dynamic file cleanup complete.")
