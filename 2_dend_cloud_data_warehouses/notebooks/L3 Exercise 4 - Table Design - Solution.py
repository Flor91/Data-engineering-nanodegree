# %% Change working directory from the workspace root to the ipynb file location. Turn this addition off with the DataScience.changeDirOnImportExport setting
# ms-python.python added
import pandas as pd
import matplotlib.pyplot as plt
import configparser
from time import time
import os
try:
    os.chdir(os.path.join(os.getcwd(), '2_dend_cloud_data_warehouses/notebooks'))
    print(os.getcwd())
except:
    pass
# %% [markdown]
# # Exercise 4: Optimizing Redshift Table Design

# %%
get_ipython().run_line_magic('load_ext', 'sql')


# %%


# %%
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))
KEY = config.get('AWS', 'key')
SECRET = config.get('AWS', 'secret')

DWH_DB = config.get("DWH", "DWH_DB")
DWH_DB_USER = config.get("DWH", "DWH_DB_USER")
DWH_DB_PASSWORD = config.get("DWH", "DWH_DB_PASSWORD")
DWH_PORT = config.get("DWH", "DWH_PORT")

# %% [markdown]
# # STEP 1: Get the params of the created redshift cluster
# - We need:
#     - The redshift cluster <font color='red'>endpoint</font>
#     - The <font color='red'>IAM role ARN</font> that give access to Redshift to read from S3

# %%
# FILL IN THE REDSHIFT ENDPOINT HERE
# e.g. DWH_ENDPOINT="redshift-cluster-1.csmamz5zxmle.us-west-2.redshift.amazonaws.com"
DWH_ENDPOINT = "dwhcluster.csmamz5zxmle.us-west-2.redshift.amazonaws.com"

# FILL IN THE IAM ROLE ARN you got in step 2.2 of the previous exercise
# e.g DWH_ROLE_ARN="arn:aws:iam::988332130976:role/dwhRole"
DWH_ROLE_ARN = "arn:aws:iam::988332130976:role/dwhRole"

# %% [markdown]
# # STEP 2: Connect to the Redshift Cluster

# %%
conn_string = "postgresql://{}:{}@{}:{}/{}".format(
    DWH_DB_USER, DWH_DB_PASSWORD, DWH_ENDPOINT, DWH_PORT, DWH_DB)
print(conn_string)
get_ipython().run_line_magic('sql', '$conn_string')

# %% [markdown]
# # STEP 3: Create Tables
# - We are going to use a benchmarking data set common for benchmarking star schemas in data warehouses.
# - The data is pre-loaded in a public bucket on the `us-west-2` region
# - Our examples will be based on the Amazon Redshfit tutorial but in a scripted environment in our workspace.
#
# ![afa](https://docs.aws.amazon.com/redshift/latest/dg/images/tutorial-optimize-tables-ssb-data-model.png)
#
# %% [markdown]
# ## 3.1 Create tables (no distribution strategy) in the `nodist` schema

# %%
get_ipython().run_cell_magic('sql', '', 'CREATE SCHEMA IF NOT EXISTS nodist;\nSET search_path TO nodist;\n\nDROP TABLE IF EXISTS part cascade;\nDROP TABLE IF EXISTS supplier;\nDROP TABLE IF EXISTS supplier;\nDROP TABLE IF EXISTS customer;\nDROP TABLE IF EXISTS dwdate;\nDROP TABLE IF EXISTS lineorder;\n\nCREATE TABLE part \n(\n  p_partkey     INTEGER NOT NULL,\n  p_name        VARCHAR(22) NOT NULL,\n  p_mfgr        VARCHAR(6) NOT NULL,\n  p_category    VARCHAR(7) NOT NULL,\n  p_brand1      VARCHAR(9) NOT NULL,\n  p_color       VARCHAR(11) NOT NULL,\n  p_type        VARCHAR(25) NOT NULL,\n  p_size        INTEGER NOT NULL,\n  p_container   VARCHAR(10) NOT NULL\n);\n\nCREATE TABLE supplier \n(\n  s_suppkey   INTEGER NOT NULL,\n  s_name      VARCHAR(25) NOT NULL,\n  s_address   VARCHAR(25) NOT NULL,\n  s_city      VARCHAR(10) NOT NULL,\n  s_nation    VARCHAR(15) NOT NULL,\n  s_region    VARCHAR(12) NOT NULL,\n  s_phone     VARCHAR(15) NOT NULL\n);\n\nCREATE TABLE customer \n(\n  c_custkey      INTEGER NOT NULL,\n  c_name         VARCHAR(25) NOT NULL,\n  c_address      VARCHAR(25) NOT NULL,\n  c_city         VARCHAR(10) NOT NULL,\n  c_nation       VARCHAR(15) NOT NULL,\n  c_region       VARCHAR(12) NOT NULL,\n  c_phone        VARCHAR(15) NOT NULL,\n  c_mktsegment   VARCHAR(10) NOT NULL\n);\n\nCREATE TABLE dwdate \n(\n  d_datekey            INTEGER NOT NULL,\n  d_date               VARCHAR(19) NOT NULL,\n  d_dayofweek          VARCHAR(10) NOT NULL,\n  d_month              VARCHAR(10) NOT NULL,\n  d_year               INTEGER NOT NULL,\n  d_yearmonthnum       INTEGER NOT NULL,\n  d_yearmonth          VARCHAR(8) NOT NULL,\n  d_daynuminweek       INTEGER NOT NULL,\n  d_daynuminmonth      INTEGER NOT NULL,\n  d_daynuminyear       INTEGER NOT NULL,\n  d_monthnuminyear     INTEGER NOT NULL,\n  d_weeknuminyear      INTEGER NOT NULL,\n  d_sellingseason      VARCHAR(13) NOT NULL,\n  d_lastdayinweekfl    VARCHAR(1) NOT NULL,\n  d_lastdayinmonthfl   VARCHAR(1) NOT NULL,\n  d_holidayfl          VARCHAR(1) NOT NULL,\n  d_weekdayfl          VARCHAR(1) NOT NULL\n);\nCREATE TABLE lineorder \n(\n  lo_orderkey          INTEGER NOT NULL,\n  lo_linenumber        INTEGER NOT NULL,\n  lo_custkey           INTEGER NOT NULL,\n  lo_partkey           INTEGER NOT NULL,\n  lo_suppkey           INTEGER NOT NULL,\n  lo_orderdate         INTEGER NOT NULL,\n  lo_orderpriority     VARCHAR(15) NOT NULL,\n  lo_shippriority      VARCHAR(1) NOT NULL,\n  lo_quantity          INTEGER NOT NULL,\n  lo_extendedprice     INTEGER NOT NULL,\n  lo_ordertotalprice   INTEGER NOT NULL,\n  lo_discount          INTEGER NOT NULL,\n  lo_revenue           INTEGER NOT NULL,\n  lo_supplycost        INTEGER NOT NULL,\n  lo_tax               INTEGER NOT NULL,\n  lo_commitdate        INTEGER NOT NULL,\n  lo_shipmode          VARCHAR(10) NOT NULL\n);')

# %% [markdown]
# ## 3.1 Create tables (with a distribution strategy) in the `dist` schema

# %%
get_ipython().run_cell_magic('sql', '', '\nCREATE SCHEMA IF NOT EXISTS dist;\nSET search_path TO dist;\n\nDROP TABLE IF EXISTS part cascade;\nDROP TABLE IF EXISTS supplier;\nDROP TABLE IF EXISTS supplier;\nDROP TABLE IF EXISTS customer;\nDROP TABLE IF EXISTS dwdate;\nDROP TABLE IF EXISTS lineorder;\n\nCREATE TABLE part (\n  p_partkey     \tinteger     \tnot nullsortkey distkey,\n  p_name        \tvarchar(22) \tnot null,\n  p_mfgr        \tvarchar(6)      not null,\n  p_category    \tvarchar(7)      not null,\n  p_brand1      \tvarchar(9)      not null,\n  p_color       \tvarchar(11) \tnot null,\n  p_type        \tvarchar(25) \tnot null,\n  p_size        \tinteger     \tnot null,\n  p_container   \tvarchar(10)     not null\n);\n\nCREATE TABLE supplier (\n  s_suppkey     \tinteger        not null sortkey,\n  s_name        \tvarchar(25)    not null,\n  s_address     \tvarchar(25)    not null,\n  s_city        \tvarchar(10)    not null,\n  s_nation      \tvarchar(15)    not null,\n  s_region      \tvarchar(12)    not null,\n  s_phone       \tvarchar(15)    not null)\ndiststyle all;\n\nCREATE TABLE customer (\n  c_custkey     \tinteger        not null sortkey,\n  c_name        \tvarchar(25)    not null,\n  c_address     \tvarchar(25)    not null,\n  c_city        \tvarchar(10)    not null,\n  c_nation      \tvarchar(15)    not null,\n  c_region      \tvarchar(12)    not null,\n  c_phone       \tvarchar(15)    not null,\n  c_mktsegment      varchar(10)    not null)\ndiststyle all;\n\nCREATE TABLE dwdate (\n  d_datekey            integer       not null sortkey,\n  d_date               varchar(19)   not null,\n  d_dayofweek\t      varchar(10)   not null,\n  d_month      \t    varchar(10)   not null,\n  d_year               integer       not null,\n  d_yearmonthnum       integer  \t not null,\n  d_yearmonth          varchar(8)\tnot null,\n  d_daynuminweek       integer       not null,\n  d_daynuminmonth      integer       not null,\n  d_daynuminyear       integer       not null,\n  d_monthnuminyear     integer       not null,\n  d_weeknuminyear      integer       not null,\n  d_sellingseason      varchar(13)    not null,\n  d_lastdayinweekfl    varchar(1)    not null,\n  d_lastdayinmonthfl   varchar(1)    not null,\n  d_holidayfl          varchar(1)    not null,\n  d_weekdayfl          varchar(1)    not null)\ndiststyle all;\n\nCREATE TABLE lineorder (\n  lo_orderkey      \t    integer     \tnot null,\n  lo_linenumber        \tinteger     \tnot null,\n  lo_custkey           \tinteger     \tnot null,\n  lo_partkey           \tinteger     \tnot null distkey,\n  lo_suppkey           \tinteger     \tnot null,\n  lo_orderdate         \tinteger     \tnot null sortkey,\n  lo_orderpriority     \tvarchar(15)     not null,\n  lo_shippriority      \tvarchar(1)      not null,\n  lo_quantity          \tinteger     \tnot null,\n  lo_extendedprice     \tinteger     \tnot null,\n  lo_ordertotalprice   \tinteger     \tnot null,\n  lo_discount          \tinteger     \tnot null,\n  lo_revenue           \tinteger     \tnot null,\n  lo_supplycost        \tinteger     \tnot null,\n  lo_tax               \tinteger     \tnot null,\n  lo_commitdate         integer         not null,\n  lo_shipmode          \tvarchar(10)     not null\n);')

# %% [markdown]
# # STEP 4: Copying tables
#
# Our intent here is to run 5 COPY operations for the 5 tables respectively as show below.
#
# However, we want to do accomplish the following:
# - Make sure that the `DWH_ROLE_ARN` is substituted with the correct value in each query
# - Perform the data loading twice once for each schema (dist and nodist)
# - Collect timing statistics to compare the insertion times
# Thus, we have scripted the insertion as found below in the function `loadTables` which
# returns a pandas dataframe containing timing statistics for the copy operations
#
# ```sql
# copy customer from 's3://awssampledbuswest2/ssbgz/customer'
# credentials 'aws_iam_role=<DWH_ROLE_ARN>'
# gzip region 'us-west-2';
#
# copy dwdate from 's3://awssampledbuswest2/ssbgz/dwdate'
# credentials 'aws_iam_role=<DWH_ROLE_ARN>'
# gzip region 'us-west-2';
#
# copy lineorder from 's3://awssampledbuswest2/ssbgz/lineorder'
# credentials 'aws_iam_role=<DWH_ROLE_ARN>'
# gzip region 'us-west-2';
#
# copy part from 's3://awssampledbuswest2/ssbgz/part'
# credentials 'aws_iam_role=<DWH_ROLE_ARN>'
# gzip region 'us-west-2';
#
# copy supplier from 's3://awssampledbuswest2/ssbgz/supplier'
# credentials 'aws_iam_role=<DWH_ROLE_ARN>'
# gzip region 'us-west-2';
# ```
#
# %% [markdown]
# ## 4.1 Automate  the copying

# %%


def loadTables(schema, tables):
    loadTimes = []
    SQL_SET_SCEMA = "SET search_path TO {};".format(schema)
    get_ipython().run_line_magic('sql', '$SQL_SET_SCEMA')

    for table in tables:
        SQL_COPY = """
copy {} from 's3://awssampledbuswest2/ssbgz/{}' 
credentials 'aws_iam_role={}'
gzip region 'us-west-2';
        """.format(table, table, DWH_ROLE_ARN)

        print(
            "======= LOADING TABLE: ** {} ** IN SCHEMA ==> {} =======".format(table, schema))
        print(SQL_COPY)

        t0 = time()
        get_ipython().run_line_magic('sql', '$SQL_COPY')
        loadTime = time()-t0
        loadTimes.append(loadTime)

        print("=== DONE IN: {0:.2f} sec\n".format(loadTime))
    return pd.DataFrame({"table": tables, "loadtime_"+schema: loadTimes}).set_index('table')


# %%
# -- List of the tables to be loaded
tables = ["customer", "dwdate", "supplier", "part", "lineorder"]

# -- Insertion twice for each schema (WARNING!! EACH CAN TAKE MORE THAN 10 MINUTES!!!)
nodistStats = loadTables("nodist", tables)
distStats = loadTables("dist", tables)

# %% [markdown]
# ## 4.1 Compare the load performance results

# %%
# -- Plotting of the timing results
stats = distStats.join(nodistStats)
stats.plot.bar()
plt.show()

# %% [markdown]
# # STEP 5: Compare Query Performance

# %%
oneDim_SQL = """
set enable_result_cache_for_session to off;
SET search_path TO {};

select sum(lo_extendedprice*lo_discount) as revenue
from lineorder, dwdate
where lo_orderdate = d_datekey
and d_year = 1997 
and lo_discount between 1 and 3 
and lo_quantity < 24;
"""

twoDim_SQL = """
set enable_result_cache_for_session to off;
SET search_path TO {};

select sum(lo_revenue), d_year, p_brand1
from lineorder, dwdate, part, supplier
where lo_orderdate = d_datekey
and lo_partkey = p_partkey
and lo_suppkey = s_suppkey
and p_category = 'MFGR#12'
and s_region = 'AMERICA'
group by d_year, p_brand1
"""

drill_SQL = """
set enable_result_cache_for_session to off;
SET search_path TO {};

select c_city, s_city, d_year, sum(lo_revenue) as revenue 
from customer, lineorder, supplier, dwdate
where lo_custkey = c_custkey
and lo_suppkey = s_suppkey
and lo_orderdate = d_datekey
and (c_city='UNITED KI1' or
c_city='UNITED KI5')
and (s_city='UNITED KI1' or
s_city='UNITED KI5')
and d_yearmonth = 'Dec1997'
group by c_city, s_city, d_year
order by d_year asc, revenue desc;
"""


oneDimSameDist_SQL = """
set enable_result_cache_for_session to off;
SET search_path TO {};

select lo_orderdate, sum(lo_extendedprice*lo_discount) as revenue  
from lineorder, part
where lo_partkey  = p_partkey
group by lo_orderdate
order by lo_orderdate
"""


def compareQueryTimes(schema):
    queryTimes = []
    for i, query in enumerate([oneDim_SQL, twoDim_SQL, drill_SQL, oneDimSameDist_SQL]):
        t0 = time()
        q = query.format(schema)
        get_ipython().run_line_magic('sql', '$q')
        queryTime = time()-t0
        queryTimes.append(queryTime)
    return pd.DataFrame({"query": ["oneDim", "twoDim", "drill", "oneDimSameDist"], "queryTime_"+schema: queryTimes}).set_index('query')


# %%
noDistQueryTimes = compareQueryTimes("nodist")
distQueryTimes = compareQueryTimes("dist")


# %%
queryTimeDF = noDistQueryTimes.join(distQueryTimes)
queryTimeDF.plot.bar()
plt.show()


# %%
improvementDF = queryTimeDF["distImprovement"] = 100.0*(
    queryTimeDF['queryTime_nodist']-queryTimeDF['queryTime_dist'])/queryTimeDF['queryTime_nodist']
improvementDF.plot.bar(title="% dist Improvement by query")
plt.show()
