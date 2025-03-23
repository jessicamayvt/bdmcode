import glob
import os
import re

import pandas as pd

import us

# BASE VARIABLES
DATA_ROOT = '/playpen/data/nbm'

##JOCELYN WROTE THIS
## helper funct - get all major releases
def get_editions():
    editions = [edition for edition in os.listdir(DATA_ROOT) if 
                (os.path.isdir(os.path.join(DATA_ROOT, edition))) 
                and "old_20230426" not in os.path.join(DATA_ROOT, edition) 
                and "corrections" not in os.path.join(DATA_ROOT, edition)
                and "preprocessed" not in os.path.join(DATA_ROOT, edition)]
    return editions


## helper funct - go through the directories to get all of the minor releases for a given major release
def get_edition_snapshots(edition):
    edition_root = os.path.join(DATA_ROOT, edition)
    snapshots = [name for name in os.listdir(edition_root) if (os.path.isdir(os.path.join(edition_root, name)) and "supporting_data" not in os.path.join(edition_root, name)) and "mobile" not in os.path.join(edition_root, name)]
    snapshots = sorted(snapshots)
    return snapshots

## JOCELYN WROTE THIS!!! function d
def get_all_releases():
    editions = get_editions
    all_releases = {}
    for edition in editions:
        all_releases[edition] = get_edition_snapshots

    return all_releases

## helper funct - get all the needed files given input major release, minor release(s), and state(s), etc
def get_nbm_files(edition, snapshots=None, states=None, ignore_satellite=False):
    res = []
    if not states:
        states = us.STATES_AND_TERRITORIES + [us.states.DC]
    if not snapshots:
        snapshots = get_edition_snapshots(edition)
    
    for s in snapshots:
        for state in states:
            snap_path = DATA_ROOT + "/" + edition + "/" + s + "/"
            files = glob.glob(snap_path + "bdc_%s*.zip" % state.fips)
            # Ensure we're only loading per-technology files. Starting in Jan 2024, the crawler pulls in per-provider files too.
            technology_filter = ["Copper", "Cable", "FibertothePremises", "FixedWireless", "Satellite", "Other"] # Handles NGSO/GSO Satellite, and LBR/Unlicensed/LicensedFixedWireless
            files = [_ for _ in files if any(tech in _ for tech in technology_filter)]
            if ignore_satellite:
                files = [_ for _ in files if "Satellite" not in _]

            res += files
    return res

## helper funct - create a dataframe from a given filepath
def df_from_nbm_file(filepath, ds=None):
    print("processing: %s" % filepath)
    df = pd.read_csv(filepath)
    match = re.search(r"bdc_(\d{2})_", filepath)
    if match:
        fips = match.group(1)
    else:
        raise ValueError("No matching FIPS code found")
    state = us.states.lookup(fips)

    df = df.rename(columns={'h3index_hex8': 'h3_res8_id', 'block_fips': 'block_geoid', 'technology_code': 'technology'}) # fix old naming scheme
    if 'state_usps' not in df:
        df['state_usps'] = state.abbr
    df = df.rename(columns={'h3_res8_id': 'h3_8'}) # make the hex column shorter...

    # Make sure block_geoids are strings
    df['block_geoid'] = df['block_geoid'].apply(lambda x: str(x).zfill(15))

    # Fixes for bad data from 20221212
    bad_provider_ids = {
        10019402494: 460515, # 20221212, Bitwise Inc 70 FIPS 26
        10002934974: 190071, # 20221212, Cable Co-op Inc 40 FIPS 39
        10018608117: 440190, # 20221212, Utah Broadband 50 FIPS 49
        10032812828: 460589, # 20221212, Quinault 70 FIPS 53 
        10028857043: 430040, # 20230216, Grice 70 FIPS 40
        10033683897: 460679 # 20221231:20230809, Newark Fiber 50 FIPS 34
    }
    df['provider_id'] = df['provider_id'].replace(bad_provider_ids)
    if ds:
        df['ds'] = ds

    # Now, right-size the memory usage of each field. Convert to nullable
    # integer types where possible to avoid float conversions.
    biz_res_code_cat = pd.api.types.CategoricalDtype(categories=['X', 'B', 'R'])
    state_usps_cat = pd.api.types.CategoricalDtype(categories=[_.abbr for _ in us.STATES_AND_TERRITORIES + [us.states.DC]])
    df = df.astype({'provider_id': 'UInt32', 
                    'location_id': 'UInt32', 
                    'technology': 'UInt8', 
                    'low_latency': 'UInt8',
                    'frn': 'UInt64',
                    'max_advertised_download_speed': 'UInt32',
                    'max_advertised_upload_speed': 'UInt32',
                    'business_residential_code': biz_res_code_cat, 
                    'state_usps': state_usps_cat})
    df.reset_index(drop=True, inplace=True)
    return df


## function a - uses the other two functions to get the entire nbm for one stat put into a dataframe
def get_state_nbm_df(edition, snapshot, state, keep_ds=False, ignore_satellite=False):
    inputs = []
    for f in get_nbm_files(edition, [snapshot], [state], ignore_satellite=ignore_satellite):
        inputs.append(df_from_nbm_file(f, snapshot))
    if keep_ds:
        return pd.concat(inputs)
    else:
        return pd.concat(inputs).drop('ds', axis=1)
    
