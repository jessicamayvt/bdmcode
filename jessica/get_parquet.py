import pandas as pd
import os
import zipfile
import pyarrow
import pyarrow.parquet as pyarrow_pq
import gc
from multiprocessing import Pool


# Dictionary mapping fips codes to states
FIPS_TO_STATE_ABBR = {
    1: 'AL', 2: 'AK', 4: 'AZ', 5: 'AR', 6: 'CA', 8: 'CO', 9: 'CT', 10: 'DE',
    11: 'DC', 12: 'FL', 13: 'GA', 15: 'HI', 16: 'ID', 17: 'IL', 18: 'IN', 19: 'IA',
    20: 'KS', 21: 'KY', 22: 'LA', 23: 'ME', 24: 'MD', 25: 'MA', 26: 'MI', 27: 'MN',
    28: 'MS', 29: 'MO', 30: 'MT', 31: 'NE', 32: 'NV', 33: 'NH', 34: 'NJ', 35: 'NM',
    36: 'NY', 37: 'NC', 38: 'ND', 39: 'OH', 40: 'OK', 41: 'OR', 42: 'PA', 44: 'RI',
    45: 'SC', 46: 'SD', 47: 'TN', 48: 'TX', 49: 'UT', 50: 'VT', 51: 'VA', 53: 'WA',
    54: 'WV', 55: 'WI', 56: 'WY'
}

# Define required columns after renaming the file
required_columns = {
    'frn', 'provider_id', 'brand_name', 'location_id', 'technology',
    'max_advertised_download_speed', 'max_advertised_upload_speed',
    'low_latency', 'business_residential_code', 'state',
    'block_geoid', 'h3_8'
}

# Defines columns that cannot contain NaN values in the file 
required_non_empty_columns = {
    'frn', 'provider_id', 'location_id', 'technology',
    'max_advertised_download_speed', 'max_advertised_upload_speed', 
    'state', 'h3_8'
}

# NBM Deadlines 
DEADLINES = ["20220630", "20221231", "20230630", "20231231", "20240630"]

# Define base input and output directories
# All parquet files can be found in the DATA_BASEDIR
DATA_BASEDIR = '/home/playpen/data/nbm_evolution/data'
base_input_dir = "/home/playpen/data/nbm"
base_output_dir = f"{DATA_BASEDIR}/nbm/bdc_single_file"
summary_output_file = os.path.join(base_output_dir, "duplicate_entries_summary.json")

# Generates a list of fips codes from the strings of keys in the dictionary 
FIPS_CODE = [str(code).zfill(2) for code in FIPS_TO_STATE_ABBR.keys()]

# Function to load an entire state's broadband map for a particular major and minor 
# release into a dataframe, and be able to filter by technology code 

# Example for Jessica to keep track: given (01, 20221212, output), it would 
# go into the directory 20221212, filter the zip files based on the fips code
# and then extract all of the files that have the fips code 01. Then all
# the csv files will be read into a dataframe. All the columns will be renamed 
# and concatenates everything into a single Parquet file. The parquet file named
# bdc_01_single_nbm.parquet

def process_fips(args):
    """
    This function processes broadband data for a given FIPS code by 
    taking all of the CSV files from the ZIP file and combining them 
    into one Parquet file. 

    @param args 
        A tuple containing three components: fips, input_dir, and output_dir
        fips is a string representing the FIPS code for the state 
        input_dir is a string representing the directory containing the zip files
        output_dir is a string representing the directory where the output Parquet file will be saved
    @return None
    """
    fips, input_dir, output_dir = args
    state_abbr = FIPS_TO_STATE_ABBR.get(int(fips), 'Unknown')
    zip_files = [file for file in os.listdir(input_dir) if file.startswith("bdc_" + fips) and file.endswith(".zip")]
    output_file_path = os.path.join(output_dir, f"bdc_{fips}_single_nbm.parquet")
    os.makedirs(output_dir, exist_ok=True)
    
    parquet_writer = None  # To hold the ParquetWriter object

    for zip_file in zip_files:
        try:
            with zipfile.ZipFile(os.path.join(input_dir, zip_file), 'r') as z:
                csv_file_name = [name for name in z.namelist() if name.endswith('.csv')][0]
                with z.open(csv_file_name) as f:
                    try:
                        csv_data = pd.read_csv(f, dtype=str)
                    except pd.errors.EmptyDataError:
                        print(f"Warning: {csv_file_name} in {zip_file} is empty or has no columns.")
                        continue
                    except Exception as e:
                        print(f"Error reading {csv_file_name} in {zip_file}: {e}")
                        continue

                    # Rename columns as specified
                    if 'state_usps' in csv_data.columns:
                        csv_data = csv_data.rename(columns={'state_usps': 'state'})
                    else:
                        csv_data['state'] = state_abbr

                    if 'block_fips' in csv_data.columns:
                        csv_data = csv_data.rename(columns={'block_fips': 'block_geoid'})

                    csv_data = csv_data.rename(columns={col: "h3_8" for col in csv_data.columns if col.startswith("h3")})
                    csv_data = csv_data.rename(columns={col: "technology" for col in csv_data.columns if col.startswith("technology")})

                    # Check if the CSV data conforms to the required columns
                    if set(csv_data.columns) != required_columns:
                        print(f"Skipping {csv_file_name} in {zip_file}: columns do not match required schema.")
                        continue

                    # Define integer columns to clean and process
                    int_columns = ['frn', 'provider_id', 'location_id', 'technology',
                                   'max_advertised_download_speed', 'max_advertised_upload_speed',
                                   'low_latency', 'block_geoid']
                    
                    # Clean and convert all integer columns
                    for col in int_columns:
                        if col in csv_data.columns:
                            csv_data[col] = csv_data[col].astype(str).replace(r"[\'\"]", "", regex=True)
                            csv_data[col] = pd.to_numeric(csv_data[col], errors='coerce')

                    # Drop rows with NaN values in any int column, then convert to Int64
                    csv_data.dropna(subset=required_non_empty_columns, inplace=True)
                    for col in int_columns:
                        if col in csv_data.columns:
                            csv_data[col] = csv_data[col].astype('Int64')

                    # Sort columns alphabetically
                    csv_data = csv_data[sorted(csv_data.columns)]

                    # Convert to PyArrow Table and write chunk
                    table = pyarrow.Table.from_pandas(csv_data)
                    if parquet_writer is None:
                        parquet_writer = pyarrow_pq.ParquetWriter(output_file_path, table.schema)
                    parquet_writer.write_table(table)

                    del table, csv_data

        except zipfile.BadZipFile:
            print(f"Warning: {zip_file} in {input_dir} is not a valid zip file. Skipping.")
            continue
        except Exception as e:
            print(f"Error processing zip file {zip_file} in {input_dir}: {e}")
            continue

    if parquet_writer:
        parquet_writer.close()
        print(f"Processed FIPS {fips} for {input_dir}")
    else:
        print(f"No valid data for FIPS {fips} in {input_dir}")

    gc.collect()


def get_all_subdirectories(base_dir):
    """
    Given a base directory, the function retrieves all of the valid subdirectories. 

    @param base_dir
        A string representing the base directory which contains subdirectories 
    @return subdirectories
        a list of tuples where each tuple contains: deadline, subrelease, and subrelease_dir
        deadline is a string refering to the name of the deadline directory 
        subrelease is a string refering to the name of the subrelease directory 
        subrelease_directory is the full path to the subrelease directory 
    """
    subdirectories = []
    for deadline in os.listdir(base_dir):
        if str(deadline) in DEADLINES:
            deadline_dir = os.path.join(base_dir, deadline)
            if os.path.isdir(deadline_dir):
                for subrelease in os.listdir(deadline_dir):
                    subrelease_dir = os.path.join(deadline_dir, subrelease)
                    if os.path.isdir(subrelease_dir):
                        subdirectories.append((deadline, subrelease, subrelease_dir))
    return subdirectories


def prepare_parquet_files():
    """
    Function that processes the date files for each FIPS code and subdirectory combination
    """
    # Collect all subdirectories for processing
    all_subdirectories = get_all_subdirectories(base_input_dir)
    
    # Prepare arguments for processing
    all_args = []
    for deadline, subrelease, input_dir in all_subdirectories:
        output_dir = os.path.join(base_output_dir, deadline, subrelease)
        for fips in FIPS_CODE:
            output_file_path = os.path.join(output_dir, f"bdc_{fips}_single_nbm.parquet")
            # Skip if output file already exists
            if os.path.exists(output_file_path):
                print(f"Skipping FIPS {fips} for {input_dir}, output file already exists: {output_file_path}")
                continue
            all_args.append((fips, input_dir, output_dir))
    
    if not all_args:
        print("No new files to process. All output files already exist.")
        return
        
    print(f"Processing {len(all_args)} FIPS code / directory combinations...")
    
    # Process with multiprocessing
    with Pool(processes=64) as pool:
        results = pool.map(process_fips, all_args)
    pool.close()
    pool.join()
    
    print("Processing completed")

if __name__ == "__main__":
    base_input_dir = f"{base_input_dir}/{20221231}/{20230524}"
    prepare_parquet_files()

# # Jessica wrote this so it might be janky lol 
# # also no safeties / error throwing implemented yet 
# # also didn't know what filter by technologies meant
# def load_into_dataframe(fips, major, minor):
    
#     # Update the input directory given input
#     base_input_dir = f"{base_input_dir}/{major}/{minor}"
#     # Prepare all files in directory and subdirectory 
#     # given the major and minor release 
#     prepare_parquet_files()

#     # Look for file in the output directory, if it exists, 
#     # read the data into a dataframe 
#     target_filename = f"bdc_{fips}_single_nbm.parquet"
#     file_path = os.path.join(base_output_dir, target_filename)
#     if os.path.exists(file_path):
#         print(f"Found file: {file_path}")
#         try:
#             # Load the Parquet file into a DataFrame
#             df = pd.read_parquet(file_path)
#             print(f"Loaded {len(df)} rows from {file_path}")
#             return df
#         except Exception as e:
#             raise RuntimeError(f"Error loading Parquet file: {e}")
#     else:
#         # File not found
#         print(f"No matching Parquet file found for FIPS {fips} in {base_output_dir}")
#         return None
