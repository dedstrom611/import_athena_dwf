import pandas as pd
import boto3
from pgcopy import CopyManager, Replace
import psycopg2
import os

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
 '''

class LoadCSVFilesToPG(object):

    def __init__(self, bucket, prefix=None):
        self.bucket = bucket
        self.prefix = prefix
        self.s3 = s3 = boto3.client('s3')

    def get_s3_keys(params={'Bucket': self.bucket, 'Prefix': self.prefix}):
        '''Get the list of keys (filenames) from an AWS S3 bucket.
        INPUTS:
        s3_obj: The name of the instantiated s3 client object.
        bucket (str): The name of the S3 bucket for which to get keys.
        prefix (str or None): If only keys from a sub-folder should be retrieved,
        prefix is the string containing the sub-folder name in which to look.
        RETURNS:
        key_list (list): A list of all keys in the S3 bucket/bucket+sub_folder
        '''
        #Use pagination in case there are more than the 1000 result limit
        paginator = self.s3.get_paginator('list_objects')
        page_iterator = paginator.paginate(params)
        d = dict()
        for page in page_iterator:
            d.update(page)
        key_list = [d['Contents'][k]['Key'] for k in range(len(d['Contents']))]
        return key_list

    def download_file_from_s3(key, filename):
        '''Download a single file from an S3 bucket and save it locally.
        INPUTS:
        s3_obj - The instantiated boto3 client object
        bucket (str): The name of the AWS s3 bucket
        key (str): The key (folder structure + file name) to retrieve
        filename (str): The filename to which the key is saved
        RETURNS: None
        '''
        self.s3.download_file(Bucket=self.bucket, Key=key, Filename=filename)

    def delete_local_file(filename):
        '''Remove a local file after it has been used. Requires import os
        INPUTS:
        filename (str): The path/name of the file to be removed
        RETURNS: None
        '''
        os.remove(filename)

    def read_csv_from_zip(zipfile, csvprefix, csv_kwargs={header: 0, skiprows: lambda x: x in [1, 2, 3]}):
        '''Retrieve a single CSV file from a zip file and return a Pandas dataframe.
        Requires import zipfile
        INPUTS:
        zipfile (str): The path/filename of the zip file
        csvprefix (str): The file prefix or entire file name to retrieve
        csv_kwars (dict): The Pandas kwargs used to read the CSV file
        RETURNS:
        df (Pandas dataframe): A Pandas dataframe of the
        '''
        zip_ref = zipfile.ZipFile(zipfile, 'r')
        for info in zip_ref.infolist():
            if csvprefix in info.filename:
                ext = zip_ref.extract(info.filename)
                df = pd.read_csv(info.filename, **csv_kwars)
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

    def load_tuples_to_postgres(pg_schema, db_table, columns):
        # Use db_wrapper or different PG SQL instantiation for this
        '''Load the CSV data to a table in PostgreSQL database.
        Requires import psycopg2 and from pgcopy import CopyManager, Replace.
        Uses a psycopg2 database connection and pgcopy for fast bulk inserts.
        INPUTS:
        pg_obj: The name of the instantiated psycopg2 object.
        pg_schema (str): The name of the PostgreSQL schema.
        db_table (str): The table in which to insert values.
        columns (tuple of strings): A tuple containing the PostgreSQL table names to insert.
        RETURNS: None
        '''
        mgr = CopyManager(pgdb_obj, pg_schema + '.' + db_table, columns)
        mgr.copy(data)
        pfdb_obj.commit()

    def load_csv_files(s3_obj, bucket, keys):
        ''' For a list of keys, read CSV files and load the results to a PostgreSQL
        database table.
        '''


if __name__ == '__main__':

dhbucket = 'dispatchhealthdata'
medhistory = 'patientpastmedicalhistory'
sochistory = 'patientsocialhistory'
s3prefix='processed/athenaftp/'

s3 = boto3.client('s3')

medical_columns = ['Past Medical History ID', 'Patient ID', 'Chart ID', 'Past Medical History Key','Past Medical History Question',\
                   'Past Medical History Answer', 'Created Datetime', 'Created By', 'Deleted Datetime', 'Deleted By']

medical_pgcols = ('id', 'patient_id', 'chart_id', 'medical_history_key', 'question', 'answer', 'created_datetime',\
                  'created_by', 'deleted_datetime', 'deleted_by')

keys = get_s3_keys(s3, dhbucket, prefix=s3prefix)

for key in keys:
    download_file_from_s3
    read_csv_from_zip
    copy_to_pgsql

df = pd.read_csv('datawarehousefeed_16.6_20180516030312_13869/'+medhistory+'_16.6_20180516030312_13869.csv', header=0, skiprows=lambda x: x in [1, 2, 3])

res = s3.download_file(Bucket='dispatchhealthdata', Key='processed/athenaftp/datawarehousefeed_16.6_20180516030312_13869.zip', Filename='athena.zip')

zip_ref = zipfile.ZipFile(filename, 'r')
for info in zip_ref.infolist():
    if medhistory in info.filename:
        ext = zipref.extract(info.filename)
        df = pd.read_csv(info.filename)


zip_ref.extract('patientpastmedicalhistory_16.6_20180516030312_13869.csv')
zip_ref.close()




medfile = 'patientpastmedicalhistory_16.6_20180516030312_13869.csv'
socfile = 'patientsocialhistory_16.6_20171212030313_13869.csv'

for info in zip_ref.infolist():
    if 'patientsocialhistory' in info.filename:
        print(info.filename)

med = pd.read_csv('patientpastmedicalhistory_16.6_20180516030312_13869.csv')


df = pd.read_csv(obj['Body'])

#Unzip the file
filepath = 'datawarehousefeed_16.6_20171212030313_13869'
filename = filepath + '.zip'



#Open the new directory and create list of file names
os.chdir(filepath)
file_list = [f for f in glob.glob('*.csv')]

# Create Pandas dataframes
for athena_file in file_list:
    #dfname, ext = os.path.splitext(athena_file)
    dfname = athena_file.split('_')[0]
    dfname = pd.read_csv(athena_file)
    print(dfname)
    print(dfname.shape)
