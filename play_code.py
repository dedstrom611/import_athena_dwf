import pandas as pd
import boto3
from pgcopy import CopyManager, Replace
import psycopg2
from psycopg2 import sql
import os
import shutil
import datetime
import zipfile
import itertools

filename = 'clinicalencounter_16.6_20180102030311_13869.csv'

# conn = psycopg2.connect("host=localhost dbname=postgres user=postgres")
# cur = conn.cursor()
with open(filename, 'r') as f:
    # Notice that we don't need the `csv` module.
    itertools.islice(f, 3, None)
    for row in f:
        print(row)
#     cur.copy_from(f, 'users', sep=',')
#
# conn.commit()
