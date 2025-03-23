import sys
import os
from get_parquet  import *
from multiprocessing import Pool
import numpy as np

# A function to categorize a BSL as served, underserved, or unserved (this is trickier!)
bsl_dict = {}
DATA_BASEDIR = '/home/playpen/data/nbm_evolution/data/nbm/bdc_single_file'

states_and_territories = [
    'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'DC', 'FL',
    'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME',
    'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH',
    'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI',
    'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY'
]

# def find_bsl_state(bsl, edition, snapshot):
#     bsl = int(bsl)
#     for fips, state_abbr in FIPS_TO_STATE_ABBR.items():
#         file_path = f"{DATA_BASEDIR}/{edition}/{snapshot}/bdc_{str(fips).zfill(2)}_single_nbm.parquet"
#         if os.path.exists(file_path):
#             try:
#                 df = pd.read_parquet(file_path, columns=["location_id", "state"])
                
#                 if (df['location_id'] == bsl).any():
#                     print("Found BSL!")
#                     state_abbr = df.loc[df["location_id"] == bsl, "state"].values[0]
#                     return state_abbr
            
#             except Exception as e:
#                 print(f"Error processing file {file_path}: {e}")
    
#     print("BSL not found in any state.")
#     return None

def process_fips(args):
    bsl, edition, snapshot, fips, state_abbr = args
    try:
        file_path = f"{DATA_BASEDIR}/{edition}/{snapshot}/bdc_{str(fips).zfill(2)}_single_nbm.parquet"
        if os.path.exists(file_path):
            df = pd.read_parquet(file_path, columns=["location_id", "state"])
            if (df['location_id'] == bsl).any():
                print(f"Found BSL in {state_abbr}!")
                return state_abbr
    except Exception as e:
        print(f"Error processing file {file_path}: {e}")
    return None

# find state given a random bsl 
def find_bsl_state(bsl, edition, snapshot):
    bsl = int(bsl)
    all_args = [(bsl, edition, snapshot, fips, state_abbr) for fips, state_abbr in FIPS_TO_STATE_ABBR.items()]

    # Use multiprocessing to check in parallel
    with Pool(processes=64) as pool:
        results = pool.map(process_fips, all_args)

    # Filter out None values and return the first match
    matched_state = next((result for result in results if result is not None), None)
    
    if matched_state:
        return matched_state
    else:
        print("BSL not found in any state.")
        return None

# find all bsls in a given state, just use len() to get count
def find_all_bsl_in_state(fips, edition, snapshot):
    # Construct the file path using the FIPS code
    file_path = f"{DATA_BASEDIR}/{edition}/{snapshot}/bdc_{str(fips).zfill(2)}_single_nbm.parquet"
    
    # Check if the file exists
    if not os.path.exists(file_path):
        print(f"Error: Data file {file_path} not found.")
        return set()
    
    try:
        # Load the Parquet file, reading only the 'location_id' column for efficiency
        df = pd.read_parquet(file_path, columns=["location_id"])
        
        # Convert 'location_id' to a set to ensure uniqueness
        bsl_set = set(df['location_id'])
        return bsl_set
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
        return set()


    
# find all the bsls in a given state and filter 
def find_filtered_bsl_in_state(fips, edition, snapshot):
    # Define valid technologies and business/residential codes
    valid_technologies = [50, 40, 10, 71]
    valid_res_codes = ["X", "R"]
    
    # Construct the file path using the FIPS code
    file_path = f"{DATA_BASEDIR}/{edition}/{snapshot}/bdc_{str(fips).zfill(2)}_single_nbm.parquet"
    
    # Check if the file exists
    if not os.path.exists(file_path):
        print(f"Error: Data file {file_path} not found.")
        return set()
    
    try:
        # Load only the necessary columns to improve performance
        df = pd.read_parquet(
            file_path,
            columns=[
                "location_id", 
                "technology", 
                "business_residential_code", 
                "low_latency"
            ]
        )
        
        # Filter based on conditions
        filtered_df = df[
            (df["technology"].isin(valid_technologies)) &  # Valid technology
            (df["business_residential_code"].isin(valid_res_codes)) &  # Valid res_code
            (df["low_latency"] == 1)  # Low latency check
        ]
        
        # Convert 'location_id' to a set to ensure uniqueness
        bsl_set = set(filtered_df["location_id"])
        
        return bsl_set
    
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
        return set()

def filtered_bsl_df(fips, edition, snapshot):
    # Define valid technologies and business/residential codes
    valid_technologies = [50, 40, 10, 71]
    valid_res_codes = ["X", "R"]
    
    # Construct the file path using the FIPS code
    file_path = f"{DATA_BASEDIR}/{edition}/{snapshot}/bdc_{str(fips).zfill(2)}_single_nbm.parquet"
    
    # Check if the file exists
    if not os.path.exists(file_path):
        print(f"Error: Data file {file_path} not found.")
        return pd.DataFrame()
    
    # Load only the necessary columns to improve performance
    df = pd.read_parquet(
        file_path,
        columns=[
                "location_id", 
                "technology", 
                "business_residential_code", 
                "low_latency",
                "max_advertised_download_speed",
                "max_advertised_upload_speed"
        ]
    )
        
    # Filter based on conditions
    filtered_df = df[
        (df["technology"].isin(valid_technologies)) &  # Valid technology
        (df["business_residential_code"].isin(valid_res_codes)) &  # Valid res_code
        (df["low_latency"] == 1)  # Low latency check
    ]
        
    return  filtered_df   

technology_codes = {
    10: "Copper Wire", # included 
    40: "Coaxial Cable / HFC", # included
    50: "Optical Carrier / Fiber to the Premises", # included
    60: "Geostationary Satellite",
    61: "Non-geostationary Satellite",
    70: "Unlicensed Terrestrial Fixed Wireless",
    71: "Licensed Terrestrial Fixed Wireless", # included
    72: "Licensed-by-Rule Terrestrial Fixed Wireless",
    0: "Other"
}

valid_technologies = [50, 40, 10, 71]
valid_res_codes = ["X", "R"]

def classify_bsl(download, upload, technology, latency, res_code):
    if technology in valid_technologies and res_code in valid_res_codes and latency == 1:
        if download >= 100 and upload >= 20:
            return "served"
        elif download >= 25 and upload >= 3: 
            return "underserved"
        else: 
            return "unserved"
    else:
        return "unserved"
    
def df_classify_bsl(df, bsl):
    if bsl is not None:
        classification = "null"
        for _, row in df[df["location_id"].astype(str) == bsl].iterrows():
            status = classify_bsl(row["max_advertised_download_speed"], row["max_advertised_upload_speed"], row["technology"], row["low_latency"], row["business_residential_code"])
            
            if status == "served":
                return "served"  # Stop searching if served
            elif status == "underserved":
                classification = "underserved"
            elif status == "unserved":
                classification = "unserved"

        return classification  # Return final classification
    return None



# I fed the code I wrote into ChatGPT and was like please speed this up, and it took the above code 
# and gave me this. idk if this is right, but 
# it gives an output that makes sense because my code i wrote is really, really slow 
# (haven't taken systems yet, not sure how to optimize these things!)
### starts here ###
def classify_bsl_vectorized(df):
    """Vectorized classification of BSLs based on download, upload, technology, and latency."""
    conditions_served = (
        (df['max_advertised_download_speed'] >= 100) &
        (df['max_advertised_upload_speed'] >= 20)
    )

    conditions_underserved = (
        (df['max_advertised_download_speed'] >= 25) &
        (df['max_advertised_upload_speed'] >= 3)
    )

    df.loc[conditions_served, 'status'] = 'served'
    df.loc[conditions_underserved & ~conditions_served, 'status'] = 'underserved'
    df.loc[~conditions_served & ~conditions_underserved, 'status'] = 'unserved'

    return df

def load_and_filter_bsl(fips, edition, snapshot):
    """Load and filter BSLs for Virginia."""
    file_path = f"{DATA_BASEDIR}/{edition}/{snapshot}/bdc_{str(fips).zfill(2)}_single_nbm.parquet"

    if not os.path.exists(file_path):
        print(f"Error: File {file_path} not found.")
        return pd.DataFrame()

    try:
        # Load relevant columns only
        df = pd.read_parquet(
            file_path,
            columns=[
                "location_id", "technology", "business_residential_code",
                "low_latency", "max_advertised_download_speed", "max_advertised_upload_speed"
            ]
        )

        # Apply initial filtering
        df_filtered = df[
            (df["technology"].isin(valid_technologies)) &
            (df["business_residential_code"].isin(valid_res_codes)) &
            (df["low_latency"] == 1)
        ]

        return df_filtered

    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return pd.DataFrame()

def count_unserved_bsl_in_virginia(majors, major_minor_dict):
    """Count unserved BSLs in Virginia for all major releases."""
    count_per_year = []

    for major in majors:
        # Load and filter the Virginia Parquet file
        df_filtered = load_and_filter_bsl(51, major, major_minor_dict[major])

        if df_filtered.empty:
            print(f"No valid BSL data for major release {major}.")
            count_per_year.append(0)
            continue

        # Classify BSLs efficiently using vectorized operations
        df_classified = classify_bsl_vectorized(df_filtered)

        # Count unserved BSLs
        unserved_count = (df_classified['status'] == 'unserved').sum()
        count_per_year.append(unserved_count)

        print(f"For major release {major} and snapshot {major_minor_dict[major]}, there are {unserved_count} unserved BSLs.")

    return count_per_year

### ends here #### 

def categorize_bsl(bsl, edition, snapshot):
    state = find_bsl_state(bsl, edition, snapshot)
    
    if state is None:
        return None
    
    fips_code = next((k for k, v in FIPS_TO_STATE_ABBR.items() if v == state), None)
    
    if fips_code is None:
        print("Error: Could not determine FIPS code for state.")
        return None
    
    file_path = f"{DATA_BASEDIR}/{edition}/{snapshot}/bdc_{str(fips_code).zfill(2)}_single_nbm.parquet"
    
    if not os.path.exists(file_path):
        print(f"Error: Data file {file_path} not found.")
        return None

    df = pd.read_parquet(file_path)
    return df_classify_bsl(df, bsl)

# if __name__ == "__main__":

#     majors = [20220630, 20221231, 20230630, 20231231, 20240630]
#     minors = [20240510, 20241104, 20250210, 20250201, 20250218]
#     major_minor_dict = dict(zip(majors, minors))
#     count_per_year = [0] * 5
#     index = 0

#     count_unserved_bsl_in_virginia(majors, major_minor_dict)


## Trying numpy

def df_classify_bsl(df):
    conditions = [
        (df["technology"].isin(valid_technologies)) & 
        (df["business_residential_code"].isin(valid_res_codes)) & 
        (df["low_latency"] == 1) & 
        (df['max_advertised_download_speed'] >= 100) & 
        (df['max_advertised_upload_speed'] >= 20),
        
        (df["technology"].isin(valid_technologies)) & 
        (df["business_residential_code"].isin(valid_res_codes)) & 
        (df["low_latency"] == 1) & 
        (df['max_advertised_download_speed'] >= 25) & 
        (df['max_advertised_upload_speed'] >= 3)
    ]
    choices = ['served', 'underserved']
    
    # Efficient classification using numpy.select
    df['status'] = np.select(conditions, choices, default='unserved')
    return df
