import pandas as pd 
import boto3
import json

from botocore.exceptions import ClientError
import configparser


config = configparser.ConfigParser()
config.read_file(open('/home/f.silvestre/Documents/Projects/Data-engineering-nanodegree/2_dend_cloud_data_warehouses/dhw.cfg'))

KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')

DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")

DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
DWH_DB                 = config.get("DWH","DWH_DB")
DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
DWH_PORT               = config.get("DWH","DWH_PORT")

DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")

(DWH_DB_USER, DWH_DB_PASSWORD, DWH_DB)

df = pd.DataFrame({"Param":
                  ["DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE", "DWH_CLUSTER_IDENTIFIER", "DWH_DB", "DWH_DB_USER", "DWH_DB_PASSWORD", "DWH_PORT", "DWH_IAM_ROLE_NAME"],
              "Value":
                  [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME]
             })

print(df)

# Create clients
ec2 = boto3.resource('ec2',
                     region_name='us-west-2', 
                     aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET)

s3 = boto3.resource('s3',
                     region_name='us-west-2', 
                     aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET)

iam = boto3.resource('iam',
                     region_name='us-west-2', 
                     aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET)

redshift = boto3.resource('redshift',
                     region_name='us-west-2', 
                     aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET)


# Connect to S3
sampleDbBucket = s3.Bucket("awssampledbuswest2")


try:
    print("Creating IAM Role")
    dwhRole=iam.create_role(
        Path='/',
        RoleName=DWH_IAM_ROLE_NAME,
        Description="Allows Redshift clusters to call AWS services on your behalf",
        AssumeRolePolicyDocument=json.dumps(
            {'Statement': [{'Action':'sts:AssumeRole',
                'Effect':'Allow',
                'Principal':{'Service': 'redshift.amazonaws.com'}}],
                'Version':'2012-10-17'}
        )
    )
except Exception as e:
    print(e)

print("Attaching policy")

iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                    PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                    )['ResponseMetadata']['HTTPStatusCode']

print("Get IAM Role")
roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

print(roleArn)

# Create Readshift cluster
try:
    response = redshift.create_cluster(
        ClusterType=DWH_CLUSTER_TYPE,
        NodeType=DWH_NODE_TYPE,
        NumberOfNodes=int(DWH_NUM_NODES),
        DBName=DWH_DB,
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
        MasterUsername=DWH_DB_USER,
        MasterUserPassword=DWH_DB_PASSWORD,
        
        #Roles (for s3 access)
        IamRoles=[roleArn]  
    )

except Exception as e:
    print(e)


# Describe cluster and status
def prettyRedshiftProps(props):
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])

myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
prettyRedshiftProps(myClusterProps)
