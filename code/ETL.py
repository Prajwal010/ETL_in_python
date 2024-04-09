import glob 
import pandas as pd 
from datetime import datetime 

log_file = "log_file.txt" 
target_file = "transformed_data.csv" 

# pandas function to extract csv
def extract_from_csv(file_to_process): 
    try:
        dataframe = pd.read_csv(file_to_process) 
        return dataframe 
    except Exception as e:
        print(f"Error reading CSV file {file_to_process}: {str(e)}")
        return None

# json
def extract_from_json(file_to_process): 
    try:
        dataframe = pd.read_json(file_to_process, lines=True) 
        return dataframe 
    except Exception as e:
        print(f"Error reading JSON file {file_to_process}: {str(e)}")
        return None

# Parquet
def extract_from_parquet(file_to_process):
    try:
        dataframe = pd.read_parquet(file_to_process) 
        return dataframe
    except Exception as e:
        print(f"Error reading Parquet file {file_to_process}: {str(e)}")
        return None

def extract():
    # Create an empty list to hold extracted data frames
    extracted_data = []

    # Process all CSV files 
    for csvfile in glob.glob("*.csv"): 
        data = extract_from_csv(csvfile)
        if data is not None:
            extracted_data.append(data)

    # Process all JSON files 
    for jsonfile in glob.glob("*.json"): 
        data = extract_from_json(jsonfile)
        if data is not None:
            extracted_data.append(data)
     
    # Process all Parquet files 
    for parquetfile in glob.glob("*.parquet"): 
        data = extract_from_parquet(parquetfile)
        if data is not None:
            extracted_data.append(data)
         
    return pd.concat(extracted_data, ignore_index=True)

def transform(data): 
    # Calculate the average Total_Bill
    avg_total_bill = data['Total_Bill'].mean()
 
    # Filter rows where Total_Bill is greater than the average
    data = data[data['Total_Bill'] > avg_total_bill]
    
    return data 

def load_data(target_file, transformed_data): 
    transformed_data.to_csv(target_file, index=False) 

def log_progress(message): 
    timestamp_format = '%Y-%m-%d %H:%M:%S' # Year-Month-Day Hour:Minute:Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open(log_file,"a") as f: 
        f.write(f"{timestamp} - {message}\n")

# Log the initialization of the ETL process 
log_progress("ETL Job Started") 
 
# Log the beginning of the Extraction process 
log_progress("Extract phase Started") 
extracted_data = extract() 
 
# Log the completion of the Extraction process 
log_progress("Extract phase Ended") 
 
# Log the beginning of the Transformation process 
log_progress("Transform phase Started") 
transformed_data = transform(extracted_data) 
print("Transformed Data:") 
print(transformed_data) 
 
# Log the completion of the Transformation process 
log_progress("Transform phase Ended") 
 
# Log the beginning of the Loading process 
log_progress("Load phase Started") 
load_data(target_file, transformed_data) 
 
# Log the completion of the Loading process 
log_progress("Load phase Ended") 
 
# Log the completion of the ETL process 
log_progress("ETL Job Ended")
