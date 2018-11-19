import pandas as pd
import boto3
from pgcopy import CopyManager, Replace
import psycopg2
from psycopg2 import sql
import os
import shutil
import datetime as dt
from datetime import timedelta
import zipfile
import athena_file_dict

'''Basic workflow:
1. Instantiate s3 client object (assumes aws credentials have been set.
Use the AWS CLI in order to set AWS key and secret_key)
2. Retrieve a list of all Keys in the S3 bucket
- read data returned from s3.list_objects_v2(Bucket=BUCKET_NAME)
- produce a list of keys using the 'Key'
    - d = s3.list_objects_v2(Bucket=BUCKET_NAME)
    - keys = [d['Contents'][k]['Key'] for k in range(len(d['Contents']))]
3. Loop through each Key:
4. Extract the required CSV file
5. Use psycopg2 copy_from to copy the data to the database table
7. Use os to remove locally loaded files
'''

# DEPRECATED
def get_s3_keys(s3_obj, bucket, prefix=None, substring=None):
    #Use pagination in case there are more than the 1000 result limit
    paginator = s3_obj.get_paginator('list_objects')
    operation_parameters = {'Bucket': bucket,
                            'Prefix': prefix}
    page_iterator = paginator.paginate(**operation_parameters)
    d = dict()
    for page in page_iterator:
        d.update(page)

    key_list = [d['Contents'][k]['Key'] for k in range(len(d['Contents']))]
    if substring is not None:
        key_list = [sub for sub in key_list if substring in sub]
    return key_list

def s3_list_files(s3_obj, bucket_name, prefix, substring=None):
    '''Get the list of keys (filenames) from an AWS S3 bucket.
    INPUTS:
    s3_obj: The name of the instantiated s3 client object.
    bucket (str): The name of the S3 bucket for which to get keys.
    prefix: (str or None): If only keys from a sub-folder should be retrieved,
    prefix is the string containing the sub-folder name in which to look.
    substring (str or None): If only keys that match a substring should be
    returned, use substring criteria.
    RETURNS:
    key_list (list): A list of all keys in the S3 bucket/bucket+sub_folder
    '''
    paginator = s3_obj.get_paginator("list_objects")

    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
    keys = []
    for page in page_iterator:
        if "Contents" in page:
            for key in page["Contents"]:
                keyString = key["Key"]
                if substring is not None:
                    if substring in keyString:
                        keys.append(keyString)

    return keys if keys else []

def download_file_from_s3(s3_obj, bucket, key, filename):
    '''Download a single file from an S3 bucket and save it locally.
    INPUTS:
    s3_obj - The instantiated boto3 client object
    bucket (str): The name of the AWS s3 bucket
    key (str): The key (folder structure + file name) to retrieve
    filename (str): The filename to which the key is saved
    RETURNS: None
    '''
    s3_obj.download_file(Bucket=bucket, Key=key, Filename=filename)

def delete_local_files(file_to_remove):
    '''Remove a local folder and S3 key after it has been used. Requires import os.
    INPUTS:
    file_to_remove (str): The file to be removed
    RETURNS: None
    '''
    os.remove(file_to_remove)
    print ('Deleted local file: {}'.format(file_to_remove))

def read_csv_from_zip(saved_zip, csvprefix, csv_params={'header': 0, 'skiprows': lambda x: x in [1, 2, 3], 'dtype': {'Deleted Datetime': str}}):
    '''Retrieve a single CSV file from a zip file and return a Pandas dataframe.
    Requires import zipfile
    INPUTS:
    zipfile (str): The path/filename of the zip file
    csvprefix (str): The file prefix or entire file name to retrieve
    csv_kwars (dict): The Pandas kwargs used to read the CSV file
    RETURNS:
    df (Pandas dataframe): A Pandas dataframe of the
    '''
    zip_ref = zipfile.ZipFile(saved_zip, 'r')
    len_prefix = len(csvprefix)
    for info in zip_ref.infolist():
        if csvprefix == info.filename[0:len_prefix]:
            ext = zip_ref.extract(info.filename)
            df = pd.read_csv(info.filename, **csv_params)
            filename = info.filename
    return filename, df

def date_func(x):
    try:
        return dt.datetime.strptime(x, '%m/%d/%Y').strftime('%Y-%m-%d')
    except:
        return pd.NaT

def datetime_func(x):
    try:
        return dt.datetime.strptime(x, '%m/%d/%Y %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')
    except:
        return pd.NaT

def reformat_datetimes(df):
    '''Reformat dates and datetimes to 'yyyy-mm-dd 00:00:00.
    If column is date, then format to YYYY-MM-DD.'''
    date_columns = [col for col in df.columns if col[-4:] == 'Date' and not df[col].isnull().all()]
    datetime_columns = [col for col in df.columns if col[-8:] == 'Datetime' and not df[col].isnull().all()]
    for dc in date_columns:
        df[dc] = df[dc].apply(date_func)
        #df[dc].fillna('1985-07-03', inplace=True)
    for dtc in datetime_columns:
        df[dtc] = df[dtc].apply(datetime_func)
        #df[dtc].fillna('1985-07-03 12:12:12', inplace=True)
    return df

def replace_missing_ids(df):
    id_columns = [col for col in df.columns if col[-2:] == 'ID' and not df[col].isnull().all()]
    for idc in id_columns:
        df[idc].fillna(0, inplace=True)
        df[idc] = df[idc].astype('int')
    return df

def replace_missing_ints(df, intlist):
    '''Integer columns with too many missing values can't be read in using pd.read_csv().
    Instead, read them as a float, replace missing values with 0 and cast as an integer.
    '''
    int_columns = [col for col in df.columns if col in intlist and not df[col].isnull().all()]
    for idc in int_columns:
        df[idc].fillna(0, inplace=True)
        df[idc] = df[idc].astype('int')
    return df

def write_formatted_csv(df, filename):
    ''' Write out a CSV with dates formatted correctly'''
    ''' Returns none '''
    df.to_csv(filename, sep='\t', date_format='%Y-%m-%d %H:%M:%S', index=False)

def get_warehouse_feed_date(filename,prefix):
    '''Strip the date from the filename and format it as YYYY-MM-DD'''
    start = filename.index(prefix) + len(prefix)
    end = start + 8
    rawdate = filename[start:end]
    sqldate = dt.datetime.strptime(rawdate, '%Y%m%d').strftime('%Y-%m-%d')
    return sqldate

def load_CSV_to_postgres(dbtable, columns, csvfile, dbhost, dbname, dbuser, dbpw):
    '''Load the CSV data to a table in PostgreSQL database.
    Requires import psycopg2 and from pgcopy import CopyManager, Replace.
    Uses a psycopg2 database connection and pgcopy for fast bulk inserts.
    INPUTS:
    pg_obj: The name of the instantiated psycopg2 object.
    pg_schema (str): The name of the PostgreSQL schema.
    dbtable (str): The table in which to insert values.
    db<credential>: The Postgres login, database and schema credentials.
    columns (tuple of strings): A tuple containing the PostgreSQL table names to insert.
    RETURNS: None
    '''
    conn = psycopg2.connect(host=dbhost,
                            dbname=dbname,
                            user=dbuser,
                            password=dbpw)
    cur = conn.cursor()
    with open(csvfile, 'r') as f:
        next(f)  # Skip the header row.
        cur.copy_from(f, dbtable, sep='\t', null='', columns=columns)
    conn.commit()

def delete_duplicates_from_pg_table(table, id):
    ''' Eventually we will use this to delete duplicate rows from a table after
    inserting data from a CSV file.'''
    sql = """DELETE FROM looker_scratch.athenadwh_provider_clone a
    USING looker_scratch.athenadwh_provider_clone b
    WHERE a.provider_id = b.provider_id
    AND a.feed_date < b.feed_date"""


def setup_parameters(file_dict, prefix):
    ''' Set up the parameters for the files to load from S3'''
    d = file_dict[prefix]['columns']
    r = file_dict[prefix]['rename']
    # Postgres table information
    postgres_table = file_dict[prefix]['postgres_table']
    # Create the list of columns by lowercasing and replacing spaces with '_'
    #columns = [k.replace(' ','_').lower() for k in d.keys()]
    csv_columns = [k for k in d.keys()]
    # Get a list of character variables, excluding any that are not datetime
    # DEPRECATED USING psycopg2 copy_from
    charvars = [k for k, v in d.items() if v == str and 'Datetime' not in k]
    # kwargs used to read the csv file
    csv_args = {'header': 0, 'skiprows': lambda x: x in [1, 2, 3], 'dtype': d}
    # Create a list of the PostgreSQL table columns to load.
    # Convert Camel case with spaces to snake case and rename columns as needed
    if r:
        pgcols = [r[key] if key in r.keys() else key.replace(' ', '_').lower() for key in d.keys()]
    else:
        pgcols = [k.replace(' ','_').lower() for k in d.keys()]

    if prefix == 'provider_':
        pgcols.append('feed_date')
    return csv_columns, csv_args, charvars, postgres_table, pgcols

if __name__ == '__main__':
    # Postgres credentials
    prod_host = 'dashboard-clone.cylxp8fwq9cz.us-west-2.rds.amazonaws.com'
    prod_db = 'dashboard'
    prod_schema = 'looker_scratch'
    prod_user = 'bi_user'
    prod_pw = '01f!uVk8cm%*'

    intlist=['Dosage Quantity', 'Prescription Fill Quantity','Number of Refills Prescribed']
    feed_version = '_17.3_'
    #Instantiate boto3 S3 client
    s3 = boto3.client('s3')

    saved_zip = 'athena.zip'
    saved_csv = 'fixed_file.csv'
    #location = '/Users/danedstrom/Documents/bi_projects/import_athena_files/'
    # CHANGE FILE LOCATION WHEN MOVING TO UBUNTU
    location = '/home/ubuntu/'
    zip_loc = location + saved_zip
    csv_loc = location + saved_csv

    substring = feed_version + dt.datetime.today().strftime('%Y%m%d')
    #substring='17.3_20181020'
    #substring = (dt.datetime.today() - datetime.timedelta(days=1)).strftime('%Y%m%d')
    #start_date = '20181105'
    #num_days = 8
    #dates = [(dt.datetime.strptime(start_date, '%Y%m%d') + timedelta(n)).strftime('%Y%m%d') for n in range(num_days)]

    # Get JSON file of Athena columns
    file_dict = athena_file_dict.get_dictionary()

    # AWS S3 information
    dhbucket = 'dispatchhealthdata'
    s3prefix='processed/athenaftp/'
    #prefixes = ['medication_', 'patientmedication_']
    prefixes = ['clinicalprovider_', 'provider_', 'clinicalencounter_', 'document_', 'patientsocialhistory', 'patientpastmedicalhistory',\
                'medication_', 'patientmedication_']

    # Obtain S3 keys from the S3 bucket
    # If only getting today's file, using the date as the substring e.g. 20180913
    keys = s3_list_files(s3, dhbucket, prefix=s3prefix, substring=substring)

    # Get keys for a specific list of dates
    #keys = [k for k in allkeys for d in dates if d in k]

    for key in keys:
        download_file_from_s3(s3, dhbucket, key, zip_loc)
        print('processing key: {}'.format(key))
        for prefix in prefixes:
            csv_columns, csv_args, charvars, postgres_table, pgcols = setup_parameters(file_dict, prefix)
            postgres_table = prod_schema + '.' + postgres_table
            df = pd.DataFrame()
            try:
                filename, df = read_csv_from_zip(saved_zip, prefix, csv_params=csv_args)
            except Exception as e:
                print ("document not in zip file")
            if not df.empty:
                feed_date = get_warehouse_feed_date(filename, feed_version)
                df = df[csv_columns]
                if prefix == 'provider_':
                    df['feed_date'] = feed_date
                # Replace any newline characters with a space
                df = df.replace('\n',' ', regex=True)
                # Check for date columns and re-format
                df = reformat_datetimes(df)
                # Replace missing ID's w/ zeroes
                df = replace_missing_ids(df)
                # Replace missing integers w/ zeroes
                df = replace_missing_ints(df, intlist)
                # Write the CSV w/ formatted datetimes
                write_formatted_csv(df, csv_loc)
                load_CSV_to_postgres(postgres_table, pgcols, csv_loc, dbhost=prod_host, dbname=prod_db, dbuser=prod_user, dbpw=prod_pw)
                os.remove(filename)
            print("Results updated for {}".format(postgres_table))
