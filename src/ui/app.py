import glob
import io
import os
import pandas as pd
import matplotlib.pyplot as plt
from flask import Flask, send_file, render_template_string, render_template
import matplotlib
from flask import url_for
import seaborn as sns
from mpl_toolkits.basemap import Basemap
import numpy as np
 
matplotlib.use('Agg')  # This line is crucial for non-GUI backend
app = Flask(__name__)
 
def clean_csv(input_file_path, output_file_path):
    with open(input_file_path, 'r', encoding='utf-8', errors='ignore') as infile, \
         open(output_file_path, 'w', encoding='utf-8') as outfile:
        for line in infile:
            corrected_line = line.strip()  # Remove leading/trailing whitespace
            # Simple check for unmatched quotes (very basic, might need adjustment)
            if corrected_line.count('"') % 2 != 0:
                corrected_line = corrected_line.replace('"', '')
            outfile.write(corrected_line + '\n')
 
input_file_path = '../../final_data/combined_cleaned_data.csv'
output_file_path = '../../final_data/cleaned_file.csv'
clean_csv(input_file_path, output_file_path)
# Load your CSV data
# Assuming this file path is accessible, adjust accordingly or use an uploaded file method
headers = [
    "Latitude", "Longitude", "UTC", "Parameter", "Concentration",
    "Unit", "RawConcentration", "AQI", "Category", "SiteName",
    "SiteAgency", "AQSID"]
 
df = pd.read_csv('../../final_data/cleaned_file.csv', low_memory=False, on_bad_lines='skip')
 
df_merged = pd.read_csv('../../final_data/merged_file.csv')
 
if len(df.columns) == len(headers):
    # Assign headers to the DataFrame
    df.columns = headers
else:
    print(f"Error: The CSV file has {len(df.columns)} columns, but {len(headers)} headers were provided.")
 
 
 
@app.route('/')
def index():
    return '''
    <html>
        <head>
            <title>Air Quality Plot Server</title>
        </head>
        <body>
            <h1>Welcome to the Air Quality Plot Server!</h1>
            <p>Select a category to view related plots:</p>
            <ul> 
                <li><a href="/processing_efficiency">Records Processing Efficiency</a></li>
                <li><a href="/air_quality">Air Quality by Location</a></li>
                <li><a href="/time_series">Time Series of Air Quality</a></li>
                <li><a href="/pollutant_distribution">Pollutant Distribution</a></li>
               
            </ul>
        </body>
    </html>
    '''
 
@app.route('/processing_efficiency')
def processing_efficiency_plot():
    try:
        fig, ax = plt.subplots()
        
        # Dynamically plotting each "RecordsProcessed-Px" column
        for column in df_merged.columns:
            sns.lineplot(data=df_merged, x=df_merged.index, y=column, ax=ax, label=column)
        
        ax.set_title('Records Processing Efficiency Over Time')
        ax.set_xlabel('Time (Index)')
        ax.set_ylabel('Records Processed')
        plt.legend()
        plt.tight_layout()
        buf = io.BytesIO()
        plt.savefig(buf, format='png')
        buf.seek(0)
        plt.close(fig)
        return send_file(buf, mimetype='image/png')
    except Exception as e:
        print(e)
        return str(e), 500
 
@app.route('/time_series')
def time_series_plot():
    try:
        # Create a copy of the dataframe to avoid modifying the original data
        local_df = df.copy()
        
        # Convert 'UTC' column to datetime format, coerce errors to NaT (Not a Time)
        local_df['UTC'] = pd.to_datetime(local_df['UTC'], errors='coerce')
 
        local_df = local_df.dropna(subset=['UTC', 'AQI'])
 
        local_df['AQI'] = pd.to_numeric(local_df['AQI'], errors='coerce')
        print(local_df['AQI'].dtype)
 
        local_df['AQI'] = local_df['AQI'].abs()
        # Sort by 'UTC' to make sure the data is in chronological order
        local_df = local_df.sort_values(by='UTC')
 
        # Create the plot
        fig, ax = plt.subplots()
        
        # Plot the data, ensure that 'UTC' is used as the x-axis and 'AQI' as the y-axis
        sns.lineplot(x='UTC', y='AQI', data=local_df, ax=ax, ci=None)
        
        # Set the title and labels of the plot
        ax.set_title('Time Series of AQI')
        ax.set_xlabel('Time')
        ax.set_ylabel('AQI')
        
        # Rotate x-axis labels to make them readable
        plt.xticks(rotation=45)
        
        # Use tight_layout to adjust the plot dimensions and layout
        plt.tight_layout()
 
        # Save the plot to a BytesIO buffer to send via Flask
        buf = io.BytesIO()
        plt.savefig(buf, format='png')
        
        # Go to the beginning of the buffer so it can be read
        buf.seek(0)
        
        # Close the plot figure to free up memory
        plt.close(fig)
        
        # Send the buffer as a response
        return send_file(buf, mimetype='image/png')
    except Exception as e:
        # In production, you would log the exception to your system's log
        print(e)
        return str(e), 500  # Return the error message and a 500 Internal Server Error status code
 
# Route for pollutant distribution
@app.route('/pollutant_distribution')
def pollutant_distribution_plot():
    # You'd need to modify this to select the right pollutant columns
    pollutants = ['PM2.5', 'PM10']  # example pollutants
    local_df = df[df['Parameter'].isin(pollutants)].copy()
    
    # Convert concentrations to numeric and handle non-numeric values
    local_df['Concentration'] = pd.to_numeric(local_df['Concentration'], errors='coerce')
    local_df.dropna(subset=['Concentration'], inplace=True)
    
    # Use a random sample of 10000 rows from the data to reduce clutter
    sample_df = local_df.sample(n=10000, random_state=1)
    
    fig, ax = plt.subplots(figsize=(10, 8))  # Increase figure size for better readability
    sns.boxplot(x='Parameter', y='Concentration', data=sample_df, ax=ax, showfliers=False)
    
    # Set log scale if needed
    # ax.set_yscale('log')
    
    ax.set_title('Distribution of Pollutant Concentrations')
    ax.set_xlabel('Pollutant')
    ax.set_ylabel('Concentration (UG/M3)')
    
    # Optionally, set the y-axis limits to focus on the main data
    # ax.set_ylim(bottom=0, top=np.percentile(local_df['Concentration'], 99))  # Adjust 99 to the desired percentile
    
    plt.tight_layout()
    buf = io.BytesIO()
    plt.savefig(buf, format='png')
    buf.seek(0)
    plt.close(fig)
    return send_file(buf, mimetype='image/png')
 
 
@app.route('/air_quality')
def plot_air_quality():
    local_df = df.copy()
    local_df['Latitude'] = pd.to_numeric(local_df['Latitude'], errors='coerce')
    local_df['Longitude'] = pd.to_numeric(local_df['Longitude'], errors='coerce')
    local_df['AQI'] = pd.to_numeric(local_df['AQI'], errors='coerce')
    local_df = local_df.dropna(subset=['Latitude', 'Longitude', 'AQI'])
    
    fig, ax = plt.subplots(figsize=(15, 10))
    
    # Create a basic map projection
    m = Basemap(projection='merc', llcrnrlat=min(local_df['Latitude']), urcrnrlat=max(local_df['Latitude']), 
                llcrnrlon=min(local_df['Longitude']), urcrnrlon=max(local_df['Longitude']), lat_ts=20, resolution='c')
    
    m.drawcoastlines()
    m.drawcountries()
    m.drawstates()
    
    x, y = m(local_df['Longitude'].values, local_df['Latitude'].values)
    sc = m.scatter(x, y, c=local_df['AQI'], cmap='viridis', vmin=0, vmax=90, s=10, edgecolor='none', alpha=0.75)
    
    plt.colorbar(sc, label='AQI', fraction=0.02, pad=0.04)  # Adjust the colorbar width here
    plt.title('Air Quality Index (AQI) by Location')
    
    buf = io.BytesIO()
    plt.savefig(buf, format='png', bbox_inches='tight', dpi=300)
    buf.seek(0)
    plt.close(fig)
    
    return send_file(buf, mimetype='image/png')
 
@app.route('/heatmap_aqi')
def heatmap_aqi():
    local_df = df.copy()
    local_df['Latitude'] = pd.to_numeric(local_df['Latitude'], errors='coerce')
    local_df['Latitude'] = local_df['Latitude'].abs()
    local_df['Longitude'] = pd.to_numeric(local_df['Longitude'], errors='coerce')
    local_df['Longitude'] = local_df['Longitude'].abs()
    local_df['AQI'] = pd.to_numeric(local_df['AQI'], errors='coerce')
    local_df['AQI'] = local_df['AQI'].abs()
    local_df = local_df.dropna(subset=['Latitude', 'Longitude', 'AQI'])
    
    fig, ax = plt.subplots()
    
    # Create a hexbin map of AQI values
    hb = ax.hexbin(local_df['Longitude'], local_df['Latitude'], C=local_df['AQI'], gridsize=50, cmap='viridis', reduce_C_function=np.mean)
    
    ax.grid(True)
    plt.colorbar(hb, label='mean(AQI)')
    ax.set_xlabel('Longitude')
    ax.set_ylabel('Latitude')
    plt.title('Heatmap of AQI')
 
    buf = io.BytesIO()
    plt.savefig(buf, format='png', bbox_inches='tight')
    buf.seek(0)
    plt.close(fig)
    
    return send_file(buf, mimetype='image/png')
 
 
def get_csv_files(directory, exclude_file='processed.csv'):
    # This function will find all csv files in the given directory excluding the 'processed.csv'
    return [f for f in glob.glob(f"{directory}/*.csv") if exclude_file not in f]
 
 
def determine_number_of_processes(dataframe):
    # The number of processes will be one less than the number of columns
    # because the first column is for seconds
    return dataframe.shape[1] - 1
 
 
def plot_records_vs_time(dataframe, num_processes):
    # Plotting function suitable for Flask, which saves the plot to a BytesIO buffer and returns it
    fig, ax = plt.subplots()
    for i in range(num_processes):
        ax.plot(dataframe['Seconds'],
                dataframe[f'RecordsProcessed-P{i}'], label=f'Process P{i}')
    ax.set_xlabel('Time (Seconds)')
    ax.set_ylabel('Number of Records Processed')
    ax.set_title('Records Processed Over Time by Each Process')
    ax.legend()
    buf = io.BytesIO()
    plt.savefig(buf, format='png')
    buf.seek(0)
    plt.close(fig)
    return buf
 
 
def load_csv(file_path):
    dataframe = pd.read_csv(file_path, low_memory=False)
    num_processes = determine_number_of_processes(dataframe)
    return dataframe, num_processes
 
 
if __name__ == '__main__':
    app.run(debug=True)