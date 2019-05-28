import pandas as pd
import boto3
from pgcopy import CopyManager, Replace
import psycopg2
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
    for info in zip_ref.infolist():
        if csvprefix in info.filename:
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

if __name__ == '__main__':

    # Postgres credentials
    prod_host = os.environ['PROD_HOST']
    prod_db = os.environ['PROD_DB']
    prod_schema = os.environ['PROD_SCHEMA']
    prod_user = ['DB_USER']
    prod_pw = ['PROD_PWD']

    #Instantiate boto3 S3 client
    s3 = boto3.client('s3')

    saved_zip = 'athena.zip'
    zip_loc = '/Users/danedstrom/Documents/bi_projects/import_athena_files/' + saved_zip

    substring = datetime.datetime.today().strftime('%Y%m%d')
    #substring = (datetime.datetime.today() - datetime.timedelta(days=1)).strftime('%Y%m%d')

    # AWS S3 information
    dhbucket = 'dispatchhealthdata'
    s3prefix='processed/athenaftp/'
    medprefix = 'patientpastmedicalhistory'

    #S3 key file column information
    medical_columns = ['Past Medical History ID', 'Patient ID', 'Chart ID', 'Past Medical History Key','Past Medical History Question',\
                       'Past Medical History Answer', 'Created Datetime', 'Created By']
    # Data types for select Pandas read_csv column inputs
    med_dict = {'Past Medical History ID':int, 'Patient ID':float, 'Chart ID':int, 'Past Medical History Key':str,\
                'Past Medical History Question':str,'Past Medical History Answer':str, 'Created Datetime':str, 'Created By':str}
    medical_char = [k for k, v in med_dict.items() if v == str and k != 'Created Datetime']

    # File name information for patient social data
    socprefix = 'patientsocialhistory'
    social_columns = ['Social History ID', 'Patient ID', 'Chart ID', 'Social History Key','Social History Name',\
                       'Social History Answer', 'Created Datetime', 'Created By']
    soc_dict = {'Social History ID':int, 'Patient ID':int, 'Chart ID':int, 'Social History Key':str,\
                'Social History Name':str,'Social History Answer':str, 'Created Datetime':str, 'Created By':str}
    social_char = [k for k, v in soc_dict.items() if v == str and k != 'Created Datetime']

    clnprefix = 'clinicalresult_'
    cln_columns = ['Clinical Result ID', 'Document ID', 'Clinical Provider ID', 'Specimen Source', 'Clinical Order Type',\
                   'Clinical Order Type Group','Created Datetime', 'Created By']

    # Data types for select Pandas read_csv column inputs
    cln_dict = {'Clinical Result ID':int, 'Document ID':int, 'Clinical Provider ID':float, 'Specimen Source':str, 'Clinical Order Type':str,\
                'Clinical Order Type Group':str, 'Clinical Order Genus':str,'Created Datetime':str, 'Created By':str}

    cln_char = [k for k, v in cln_dict.items() if v == str and k not in ['Created Datetime']]

    # kwargs used to read the medical and social csv file
    med_args = {'header': 0, 'skiprows': lambda x: x in [1, 2, 3], 'dtype': med_dict}
    soc_args = {'header': 0, 'skiprows': lambda x: x in [1, 2, 3], 'dtype': soc_dict}
    cln_args = {'header': 0, 'skiprows': lambda x: x in [1, 2, 3], 'dtype': cln_dict}

    # Postgres table information
    medtable = 'athenadwh_medical_history_clone'
    soctable = 'athenadwh_social_history_clone'
    clntable = 'athenadwh_clinical_results_clone'
    medical_pgcols = ('id', 'patient_id', 'chart_id', 'medical_history_key', 'question', 'answer', 'created_datetime','created_by')
    social_pgcols = ('id', 'patient_id', 'chart_id', 'social_history_key', 'question', 'answer', 'created_datetime','created_by')
    cln_pgcols = ('clinical_result_id','document_id','clinical_provider_id','specimen_source','clinical_order_type','clinical_order_type_group',\
                  'created_datetime','created_by')

    keys = get_s3_keys(s3, dhbucket, prefix=s3prefix, substring=substring)

    # load_csv_files_to_pg(s3, keys, dhbucket, medprefix, medical_columns, saved_zip, med_args, medtable, medical_pgcols,
    # dbhost=prod_host, dbname=prod_db, dbschema=prod_schema, dbuser=prod_user, dbpw=prod_pw)
    #
    # load_csv_files_to_pg(s3, keys, dhbucket, socprefix, social_columns, saved_zip, soc_args, soctable, social_pgcols,
    # dbhost=prod_host, dbname=prod_db, dbschema=prod_schema, dbuser=prod_user, dbpw=prod_pw)
    '''
    for key in keys:
        download_file_from_s3(s3, dhbucket, key, zip_loc)
        filename, df = read_csv_from_zip(saved_zip, medprefix, csv_params=med_args)
        if not df.empty:
            df = reformat_datetime(df, ['Created Datetime'])
            df = encode_to_binary(df, medical_char)
            medtuple = df_to_tuples(df, medical_columns)
            load_tuples_to_postgres(medtable, medical_pgcols, medtuple, dbhost=prod_host, dbname=prod_db, dbschema=prod_schema, dbuser=prod_user, dbpw=prod_pw)
        os.remove(filename)

        filename, df = read_csv_from_zip(saved_zip, socprefix, csv_params=soc_args)
        if not df.empty:
            df = reformat_datetime(df, ['Created Datetime'])
            df = encode_to_binary(df, social_char)
            soctuple = df_to_tuples(df, social_columns)
            load_tuples_to_postgres(soctable, social_pgcols, soctuple, dbhost=prod_host, dbname=prod_db, dbschema=prod_schema, dbuser=prod_user, dbpw=prod_pw)
        os.remove(filename)

        df = pd.DataFrame()
        try:
            filename, df = read_csv_from_zip(saved_zip, clnprefix, csv_params=cln_args)
        except Exception as e:
            print ("document not in zip file")
        if not df.empty:
            df = reformat_datetime(df, ['Created Datetime'])
            df = encode_to_binary(df, cln_char)
            for v in ['Clinical Provider ID']:
                df[v] = df[v].fillna(-1)
                df[v] = df[v].astype('int')

            clntuple = df_to_tuples(df, cln_columns)
            load_tuples_to_postgres(clntable, cln_pgcols, clntuple, dbhost=prod_host, dbname=prod_db, dbschema=prod_schema, dbuser=prod_user, dbpw=prod_pw)
            os.remove(filename)
    '''
