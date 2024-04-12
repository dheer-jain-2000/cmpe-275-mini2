import os

def write_contents_and_filenames_to_new_file(folder_path, output_file_path):
    # Construct the path to the directory you want to skip
    skip_path = os.path.join(folder_path, 'ui/venv')  # Adjust this as needed

    with open(output_file_path, 'w', encoding='utf-8') as output_file:
        # Walk through all directories and files in the folder
        for root, dirs, files in os.walk(folder_path):
            # Check if the current directory is or is under the directory to skip
            if root.startswith(skip_path):
                continue  # Skip this directory and its files
            
            for file in files:
                # Skip if the file is a .csv file
                if file.endswith('.csv'):
                    continue

                file_path = os.path.join(root, file)
                try:
                    # Read the content of the current file
                    with open(file_path, 'r', encoding='utf-8') as current_file:
                        content = current_file.read()

                    # Write the file name and its content to the output file
                    output_file.write(f"File Name: {file}\n{content}\n")
                    output_file.write("-" * 80 + "\n")  # Separator between files

                except Exception as e:
                    print(f"Could not process {file_path}: {e}")

    print(f"Contents have been written to {output_file_path}")

# Example usage
folder_path = '/Users/spartan/Documents/SJSU/Sem2/CMPE-275/Mini2/Final_code/CMPE-275-HPC/src'  # Folder containing the files you want to process
output_file_path = '/Users/spartan/Documents/SJSU/Sem2/CMPE-275/Mini2/Final_code/CMPE-275-HPC/output_file.txt'  # Path where you want to save the new file
write_contents_and_filenames_to_new_file(folder_path, output_file_path)
