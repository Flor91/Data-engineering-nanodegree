#%% Change working directory from the workspace root to the ipynb file location. Turn this addition off with the DataScience.changeDirOnImportExport setting
# ms-python.python added
import os
try:
	os.chdir(os.path.join(os.getcwd(), '2_dend_cloud_data_warehouses/notebooks'))
	print(os.getcwd())
except:
	pass
#%% [markdown]
# # Exercise 3: Parallel ETL

#%%
get_ipython().run_line_magic('load_ext', 'sql')


#%%
from time import time
import configparser
import matplotlib.pyplot as plt
import pandas as pd

#%% [markdown]
# # STEP 1: Get the params of the created redshift cluster 
# - We need:
#     - The redshift cluster <font color='red'>endpoint</font>
#     - The <font color='red'>IAM role ARN</font> that give access to Redshift to read from S3

#%%
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))
KEY=config.get('AWS','key')
SECRET= config.get('AWS','secret')

DWH_DB= config.get("DWH","DWH_DB")
DWH_DB_USER= config.get("DWH","DWH_DB_USER")
DWH_DB_PASSWORD= config.get("DWH","DWH_DB_PASSWORD")
DWH_PORT = config.get("DWH","DWH_PORT")


#%%
# FILL IN THE REDSHIFT ENPOINT HERE
# e.g. DWH_ENDPOINT="redshift-cluster-1.csmamz5zxmle.us-west-2.redshift.amazonaws.com" 
DWH_ENDPOINT="" 
    
#FILL IN THE IAM ROLE ARN you got in step 2.2 of the previous exercise
#e.g DWH_ROLE_ARN="arn:aws:iam::988332130976:role/dwhRole"
DWH_ROLE_ARN=""

#%% [markdown]
# # STEP 2: Connect to the Redshift Cluster

#%%
conn_string="postgresql://{}:{}@{}:{}/{}".format(DWH_DB_USER, DWH_DB_PASSWORD, DWH_ENDPOINT, DWH_PORT,DWH_DB)
print(conn_string)
get_ipython().run_line_magic('sql', '$conn_string')


#%%
import boto3

s3 = boto3.resource('s3',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                     )

sampleDbBucket =  s3.Bucket("udacity-labs")

for obj in sampleDbBucket.objects.filter(Prefix="tickets"):
    print(obj)

#%% [markdown]
# # STEP 3: Create Tables

#%%
get_ipython().run_cell_magic('sql', '', 'DROP TABLE IF EXISTS "sporting_event_ticket";\nCREATE TABLE "sporting_event_ticket" (\n    "id" double precision DEFAULT nextval(\'sporting_event_ticket_seq\') NOT NULL,\n    "sporting_event_id" double precision NOT NULL,\n    "sport_location_id" double precision NOT NULL,\n    "seat_level" numeric(1,0) NOT NULL,\n    "seat_section" character varying(15) NOT NULL,\n    "seat_row" character varying(10) NOT NULL,\n    "seat" character varying(10) NOT NULL,\n    "ticketholder_id" double precision,\n    "ticket_price" numeric(8,2) NOT NULL\n);')

#%% [markdown]
# # STEP 4: Load Partitioned data into the cluster

#%%
get_ipython().run_cell_magic('time', '', 'qry = """\n    copy sporting_event_ticket from \'s3://udacity-labs/tickets/split/part\'\n    credentials \'aws_iam_role={}\'\n    gzip delimiter \';\' compupdate off region \'us-west-2\';\n""".format(DWH_ROLE_ARN)\n\n%sql $qry')

#%% [markdown]
# # STEP 4: Create Tables for the non-partitioned data

#%%
get_ipython().run_cell_magic('sql', '', 'DROP TABLE IF EXISTS "sporting_event_ticket_full";\nCREATE TABLE "sporting_event_ticket_full" (\n    "id" double precision DEFAULT nextval(\'sporting_event_ticket_seq\') NOT NULL,\n    "sporting_event_id" double precision NOT NULL,\n    "sport_location_id" double precision NOT NULL,\n    "seat_level" numeric(1,0) NOT NULL,\n    "seat_section" character varying(15) NOT NULL,\n    "seat_row" character varying(10) NOT NULL,\n    "seat" character varying(10) NOT NULL,\n    "ticketholder_id" double precision,\n    "ticket_price" numeric(8,2) NOT NULL\n);')

#%% [markdown]
# # STEP 5: Load non-partitioned data into the cluster
# - Note how it's slower than loading partitioned data

#%%
get_ipython().run_cell_magic('time', '', '\nqry = """\n    copy sporting_event_ticket_full from \'s3://udacity-labs/tickets/full/full.csv.gz\' \n    credentials \'aws_iam_role={}\' \n    gzip delimiter \';\' compupdate off region \'us-west-2\';\n""".format(DWH_ROLE_ARN)\n\n%sql $qry')


#%%



