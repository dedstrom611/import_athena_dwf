import pandas as pd
import boto3
from pgcopy import CopyManager, Replace
import psycopg2
from psycopg2 import sql
import os
import shutil
import datetime
import zipfile

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
5. Read the csv into a Pandas dataframe
6. Use psycopg2 and pgcopy CopyManager to copy the data to the database table
7. Use os to remove locally loaded files
'''

def get_s3_keys(s3_obj, bucket, prefix=None, substring=None):
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

def encode_to_binary(df, cols):
    '''Encode all strings to binary, per the requirements of pgcopy. '''
    for col in cols:
        df[col] = df[col].apply(lambda x: str(x).encode())
    return df

def reformat_datetime(df, cols):
    '''Reformat datetimes to 'yyyy-mm-dd 00:00:00'''
    for col in cols:
        df[col] = pd.to_datetime(df[col], infer_datetime_format=True)
    return df

def df_to_tuples(df, cols):
    '''Take in a Pandas dataframe and a list of variable names
    and return a list of tuples containing the data in the columns
    in the order of the input columns
    INPUTS:
    df (Pandas dataframe): The pandas dataframe from which to read
    cols (list of strings): The list of columns in the dataframe in which
    create the list of tuples
    RETURNS:
    tuplelist (list of tuples): A list of tuples
    '''
    subset = df[cols].where(pd.notnull(df), None)
    tuplelist = [tuple(x) for x in subset.values]
    return tuplelist

def load_tuples_to_postgres(db_table, columns, data, dbhost, dbname, dbschema, dbuser, dbpw):
    '''Load the CSV data to a table in PostgreSQL database.
    Requires import psycopg2 and from pgcopy import CopyManager, Replace.
    Uses a psycopg2 database connection and pgcopy for fast bulk inserts.
    INPUTS:
    pg_obj: The name of the instantiated psycopg2 object.
    pg_schema (str): The name of the PostgreSQL schema.
    db_table (str): The table in which to insert values.
    db<credential>: The Postgres login, database and schema credentials.
    columns (tuple of strings): A tuple containing the PostgreSQL table names to insert.
    RETURNS: None
    '''
    connection = psycopg2.connect(host=dbhost,
                                 dbname=dbname,
                                 user=dbuser,
                                 password=dbpw)
    mgr = CopyManager(connection, dbschema + '.' + db_table, columns)
    mgr.copy(data)
    connection.commit()

def load_csv_files_to_pg(s3_obj, keys, bucket, s3prefix, s3columns, localzipfile, csv_args, dbtable, dbcolumns, dbhost, dbname, dbschema, dbuser, dbpw):
    """ For a list of keys, read CSV files and load the results to a PostgreSQL
    database table.
    """
    for key in keys:
        download_file_from_s3(s3_obj, bucket, key, localzipfile)
        filename, df = read_csv_from_zip(localzipfile, s3prefix, csv_params=csv_args)
        if not df.empty:
            df = reformat_datetime(df, ['Created Datetime'])
            df = encode_to_binary(df, medical_char)
            tuples = df_to_tuples(df, s3columns)
            load_tuples_to_postgres(dbtable, dbcolumns, tuples, dbhost, dbname, dbschema, dbuser, dbpw)
        os.remove(filename)

def get_dictionary():
    file_dict = {
      'provider_': {
        'columns':{
          'Provider ID':int,
          'Provider First Name':str,
          'Provider Last Name':str,
          'Provider User Name':str,
          'Provider Type':str,
          'Provider Type Name':str,
          'Provider Type Category':str,
          'Provider NPI Number':str,
          'Provider Group ID':int,
          'Supervising Provider ID':int,
          'Taxonomy':str,
          'Specialty':str,
          'Created Datetime':str,
          'Deleted By':str
        },
        'rename': dict(),
        'postgres_table': 'athenadwh_provider_clone'
        },
        'patientpastmedicalhistory': {
          'columns':{
            'Past Medical History ID':int,
            'Patient ID':float,
            'Chart ID':int,
            'Past Medical History Key':str,
            'Past Medical History Question':str,
            'Past Medical History Answer':str,
            'Created Datetime':str,
            'Created By':str
          },
          'rename': {
            'Past Medical History ID':'id',
            'Past Medical History Key':'medical_history_key',
            'Past Medical History Question':'question',
            'Past Medical History Answer':'answer'
          },
          'postgres_table': 'athenadwh_medical_history_clone'
        },
        'patientsocialhistory': {
          'columns':{
            'Social History ID':int,
            'Patient ID':int,
            'Chart ID':int,
            'Social History Key':str,
            'Social History Name':str,
            'Social History Answer':str,
            'Created Datetime':str,
            'Created By':str
          },
          'rename':{
            'Social History ID':'id',
            'Social History Name':'question',
            'Social History Answer':'answer'
          },
          'postgres_table': 'athenadwh_social_history_clone'
        },
        'clinicalresult_': {
          'columns':{
            'Clinical Result ID':int,
            'Document ID':int,
            'Clinical Provider ID':float,
            'Specimen Source':str,
            'Clinical Order Type':str,
            'Clinical Order Type Group':str,
            'Clinical Order Genus':str,
            'Created Datetime':str,
            'Created By':str
            },
          'rename': dict(),
          'postgres_table': 'athenadwh_clinical_results_clone'
        },
        'document_': {
          'columns':{
            'Document ID':int,
            'Patient ID':float,
            'Chart ID':float
          },
          'rename':dict(),
          'postgres_table': 'athenadwh_document_crosswalk_clone'
        },
        'clinicalprovider_': {
          'columns': {
            'Clinical Provider ID': int,
            'Fax': str
          },
          'rename': dict(),
          'postgres_table': 'athenadwh_clinical_providers_fax_clone'
        },
        'clinicalencounter_': {
          'columns': {
            'Clinical Encounter ID': int,
            'Patient ID': int,
            'Chart ID': int,
            'Appointment ID': float,
            'Provider ID': int,
            'Encounter Date': str,
            'Encounter Status': str,
            'Created Datetime': str,
            'Closed Datetime': str,
            'Closed By': str
          },
          'rename': dict(),
          'postgres_table': 'clinical_encounters_transactions'
        }
    }
    return file_dict

def setup_parameters(file_dict, prefix):
    ''' Set up the parameters for the files to load from S3'''
    d = file_dict[prefix]['columns']
    # Postgres table information
    postgres_table = file_dict[prefix]['postgres_table']
    # Create the list of columns by lowercasing and replacing spaces with '_'
    #columns = [k.replace(' ','_').lower() for k in d.keys()]
    csv_columns = [k for k in d.keys()]
    # Get a list of character variables, excluding any that are not datetime
    charvars = [k for k, v in d.items() if v == str and 'Datetime' not in k]
    # kwargs used to read the csv file
    csv_args = {'header': 0, 'skiprows': lambda x: x in [1, 2, 3], 'dtype': d}
    # Create a tuple of the PostgreSQL table columns.  This is used by pgcopy
    pgcols = tuple(k.replace(' ','_').lower() for k in d.keys())
    return csv_columns, csv_args, charvars, postgres_table, pgcols

if __name__ == '__main__':
    # Postgres credentials
    prod_host = 'dashboard-clone.cylxp8fwq9cz.us-west-2.rds.amazonaws.com'
    prod_db = 'dashboard'
    prod_schema = 'looker_scratch'
    prod_user = 'bi_user'
    prod_pw = '01f!uVk8cm%*'

    #Instantiate boto3 S3 client
    s3 = boto3.client('s3')

    saved_zip = 'athena.zip'
    location = '/Users/danedstrom/Documents/bi_projects/import_athena_files/'
    #location = '/home/ubuntu/'
    zip_loc = location + saved_zip

    substring = datetime.datetime.today().strftime('%Y%m%d')
    substring = (datetime.datetime.today() - datetime.timedelta(days=1)).strftime('%Y%m%d')

    # Get JSON file of Athena columns
    file_dict = get_dictionary()

    # AWS S3 information
    dhbucket = 'dispatchhealthdata'
    s3prefix='processed/athenaftp/'
    prefixes = ['clinicalprovider_', 'provider_']
    # prefixes = ['clinicalencounter_']

    # Obtain S3 keys from the S3 bucket
    # If only getting today's file, using the date as the substring e.g. 20180913
    keys = get_s3_keys(s3, dhbucket, prefix=s3prefix, substring=None)

    '''
    for prefix in prefixes:
        csv_columns, csv_args, charvars, postgres_table, pgcols = setup_parameters(file_dict, prefix)

        for key in keys:
            download_file_from_s3(s3, dhbucket, key, zip_loc)
            df = pd.DataFrame()
            try:
                filename, df = read_csv_from_zip(saved_zip, prefix, csv_params=csv_args)
            except Exception as e:
                print ("document not in zip file")
            if not df.empty:
                # Check for date columns and re-format
                for column in df.columns:
                    if 'Datetime' in column:
                        df = reformat_datetime(df, ['Created Datetime', 'Closed Datetime'])
                    # Check for ID columns, set null value and create as integer
                    if 'ID' in column:
                        df[column] = df[column].astype('int')
                        df[column] = df[column].fillna(-1)
                df = encode_to_binary(df, charvars)

                pgtuple = df_to_tuples(df, csv_columns)
                load_tuples_to_postgres(postgres_table, pgcols, pgtuple, dbhost=prod_host, dbname=prod_db, dbschema=prod_schema, dbuser=prod_user, dbpw=prod_pw)
                os.remove(filename)
    '''
    prefix=prefixes[0]
    key = keys[500]
    csv_columns, csv_args, charvars, postgres_table, pgcols = setup_parameters(file_dict, prefix)


    download_file_from_s3(s3, dhbucket, key, zip_loc)
    '''    df = pd.DataFrame()
    try:
            zip_ref = zipfile.ZipFile(saved_zip, 'r')
            len_prefix = len(prefix)
    for info in zip_ref.infolist():
        if prefix == info.filename[0:len_prefix]:
            print(info.filename)
                        ext = zip_ref.extract(info.filename)
                        filename = 'clinicalencounter_16.6_20180102030311_13869.csv'
                        df = pd.read_csv(filename, **csv_args)
                        filename = info.filename
                        skipinitialspace=True).fillna('')
            filename, df = read_csv_from_zip(saved_zip, prefix, csv_params=csv_args)
        except Exception as e:
            print ("document not in zip file")
        if not df.empty:
            # Check for date columns and re-format

    df = pd.read_csv(filename, **csv_args)
    for column in df.columns:
        if 'Datetime' in column:
            df = reformat_datetime(df, [column])
        # Check for ID columns, set null value and create as integer
        if 'ID' in column:
            df[column] = df[column].fillna(-1)
            df[column] = df[column].astype('int')'''

    '''    bin_df = encode_to_binary(df, charvars)

    pgtuple = df_to_tuples(bin_df, csv_columns)
    load_tuples_to_postgres(postgres_table, pgcols, pgtuple, dbhost=prod_host, dbname=prod_db, dbschema=prod_schema, dbuser=prod_user, dbpw=prod_pw)

            os.remove(filename)


        cur.execute(
            cur = conn.cursor()
            insert_sql = sql.SQL("INSERT INTO {0} VALUES ({1}) ON CONFLICT ({2}) DO UPDATE {0} SET {3}").format(
                         sql.Identifier('table_name'),
                         sql.SQL(', ').join(map(sql.Identifier, eventdata.keys()) ))

            cur.execute( insert_sql.as_string(conn) % tuple(eventdata.values()) )
            cur.close()
            conn.commit()

    df_final = df[csv_columns]
    connection = psycopg2.connect(host=prod_host,
                                 dbname=prod_db,
                                 user=prod_user,
                                 password=prod_pw)
    with connection.cursor() as cursor:
        for i, row in df_final.iterrows():
            sql = """INSERT INTO looker_scratch.clinical_encounters_transactions
            (clinical_encounter_id,chart_id,appointment_id,provider_id,encounter_date,encounter_status,created_datetime,closed_datetime,closed_by)
            VALUES ('{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}', '{8}')
            ON CONFLICT (clinical_encounter_id) DO UPDATE
              SET
                  closed_datetime = excluded.closed_datetime,
                  closed_by = excluded.closed_by"""
            cursor.execute(sql, (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8]))
            connection.commit()
    connection.close()'''
