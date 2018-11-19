import pysftp
import boto3

# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#client

hostname='sftp.dispatchhealth.com'
username='ubuntu'
keyfile='/Users/danedstrom/Documents/FredKeyPair.pem'

athena_file='datawarehousefeed_17.3_20181104080514_13869.zip'

''' Basic workflow:
1. Copy file from sftp site
2. Process file using import_athena_csv_to_postgres
3. Copy file to S3 bucket
4. Remove file from sftp site.
'''

# Copy file from SFTP site
with pysftp.Connection(hostname, username=username, private_key=keyfile) as sftp:
    with sftp.cd('../athenauser/inbound/'):             # temporarily chdir to public
        #sftp.put('/my/local/filename')  # upload file to public/ on remote
        sftp.get(athena_file)         # get a remote file

# Copy file to S3 bucket
dhbucket = 'dispatchhealthdata'
s3prefix='processed/athenaftp/'

s3 = boto3.client('s3')
s3.upload_file(athena_file,dhbucket,athena_file)

# Example of removing a remote file
sftp.remove(file_to_remove)
