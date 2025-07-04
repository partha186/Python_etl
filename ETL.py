from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.types import *


args = getResolvedOptions(sys.argv, ['MY_JOB_NAME','JDBC_URL', 'USERNAME', 'PASSWORD', 
                                     'POLICY', 'PERSONS','CALIMS'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['MY_JOB_NAME'], args)

jdbc_url = args['JDBC_URL']
username = args['USERNAME']
password = args['PASSWORD']
table1= args['POLICY']
table2 = args['PERSONS']
table3 = args['CALIMS']
aws_region = args['AWS_REGION']

#reading the data from database by usinf jdbc connection
df1 = spark.read.format("jdbc").options(
    url=jdbc_url,
    dbtable=table1,
    user=username,
    password=password,
    driver="com.teradata.jdbc.TeraDriver"
).load()

df2 = spark.read.format("jdbc").options(
    url=jdbc_url,
    dbtable=table2,
    user=username,
    password=password,
    driver="com.teradata.jdbc.TeraDriver"
).load()

df3= spark.read.format("jdbc").options(
    url=jdbc_url,
    dbtable=table3,
    user=username,
    password=password,
    driver="com.teradata.jdbc.TeraDriver"
).load()
# s3 bucket that cotains state code 
state_cd="s3_bucket_state_cd_path"

#s3 bucket that contains account_number & info_key informnation
info_syskey='s3_info_key_path'

# removing the null value columns for three table 
df1=df1.dropna(how="all") 
df2=df2.dropna(how="all")
df3=df3.dropna(how="all")
#filtering the data based on date to get latest data
df1=df1.filter(df1['sys_date']>='date')
df2=df2.filter(df1['sys_date']>='date')
df3=df3.filter(df1['sys_date']>='date')
# joining the three table based in account_id
joined_data=df1.join(df2,df1["acc_nbr"]==df2["acc_nbr'],"inner").join(df3,df1["acc_nbr"]==df3["acc_nbr'],"inner")

df_s3 = spark.read.option("header", "true").csv(info_syskey) 
#converting the columns of info_key to integer bzc in s3 csv formate the data will in string formate
df_s3 = df_s3.withColumn("s_account_number", col("s_account_number").cast("int"))
df_s3 = df_s3.withColumn("s_syskey", col("s_syskey").cast("int"))

# joining the df_s3 and joined_data 
join_info_key_data=joined_data.join(df_s3,df_s3["account_number"]=joined_data['acc_nbr'],"left")

# Get all existing syskeys in a set (to avoid duplicates)
existing_syskeys = set(row["syskey"] for row in df_s3.select("syskey").distinct().collect())


# Function to generate a unique random syskey
def generate_unique_syskey():
    while True:
        new_syskey = random.randint(900000, 999999)  # Generate a random key
        if new_syskey not in existing_syskeys:  # Ensure it's not a duplicate
            existing_syskeys.add(new_syskey)  # Add to existing keys
            return new_syskey


generate_syskey_udf = udf(generate_unique_syskey, IntegerType()) 

join_info_key_data=join_info_key_data.withColumn("syskey",when(col("syskey").isNotNull(), col("syskey")).otherwise(generate_syskey_udf()))

new_records = join_info_key_data.filter(col("syskey").isNotNull()).select("acc_nbr", "syskey")
# adding newly generated syskey in s3 file
if new_records.count() > 0:
    new_records.write.mode("append").option("header", "true").csv(info_syskey)
# drop the s_account_number column
    
join_info_key_data=join_info_key_data.drop(join_info_key_data["s_account_number"])

state_s3 = spark.read.option("header", "true").csv(state_cd) 

df_final=join_info_key_data.join(state_s3,"state","left")

df_final=df_final.drop("state")
dest_path="s3_destinarion_path"

df_final.write.mode("overwrite").parquet(dest_path)

job.commit()
