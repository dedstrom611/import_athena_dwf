import pysftp
import os

with pysftp.Connection(sftp_host, username=sftp_user, private_key=pemfile) as sftp:
    with sftp.cd('public'):             # temporarily chdir to public
        sftp.put('/my/local/filename')  # upload file to public/ on remote
        sftp.get('remote_file')         # get a remote file

sftp_host=sftp.dispatchhealth.com
sftp_user=ubuntu
pemfile=/Users/danedstrom/Documents/FredKeyPair.pem

sftp_host=os.environ['SFTP_HOST']
sftp_user = os.environ['SFTP_USER']
pemfile = os.environ['UBUNTU_PEMLOCATION']
