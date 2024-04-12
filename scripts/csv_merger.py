import pandas as pd

def merge_csv_files(process_count):
    csv_files_directory = '../data/'
    dfs = []  # To store the loaded DataFrames

    # Dynamically load CSV files based on the number of processes
    for i in range(process_count):
        file_path = f'{csv_files_directory}records_per_second_process_{i}.csv'
        df = pd.read_csv(file_path)
        dfs.append(df)
    
    # Merge the DataFrames on the 'Seconds' column
    merged_df = dfs[0]
    for i in range(1, len(dfs)):
        merged_df = merged_df.merge(dfs[i], on='Seconds', how='outer', suffixes=('', f'-P{i}'))
    
    # Dynamically generate new column names
    column_names = ['Seconds'] + [f'RecordsProcessed-P{i}' for i in range(process_count)]
    merged_df.columns = column_names
    
    # Fill missing values with 0, assuming missing values should be treated as 0
    merged_df.fillna(0, inplace=True)

    # Sort the dataframe based on 'Seconds' column
    merged_df.sort_values(by='Seconds', inplace=True)

    # Reset index after sorting
    merged_df.reset_index(drop=True, inplace=True)

    # Save the merged dataframe to a new CSV file
    merged_df.to_csv('../final_data/merged_file.csv', index=False)

    print("Merging complete. The merged data is saved as 'merged_file.csv'.")

# Ask the user for the number of processes
process_count = int(input("Enter the number of processes: "))
merge_csv_files(process_count)
