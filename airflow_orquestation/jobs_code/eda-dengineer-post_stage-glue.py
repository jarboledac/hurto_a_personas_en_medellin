import sys
from awsglue.transforms import *
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions
import awswrangler as wr


glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

args = getResolvedOptions(sys.argv, [ 'BUCKET_PRESTAGE','PREFIX'])

prefix = args['PREFIX']
bucket_prestage = args['BUCKET_PRESTAGE']


#LOAD DATA
print(f"##### START READING DATA FILE ####")
datasource0 = glueContext.create_dynamic_frame_from_options(
            connection_type = "s3", 
            connection_options = {"paths": [f"s3://{bucket_prestage}/pre-stage/{prefix}.csv"]}, 
            format="csv", 
            format_options = {"withHeader": True,
            "separator": "|"})
            
#READING DATA IN A DATAFRAME      
print(f"#### CONVERTING DYNAMIC FRAME TO DATAFRAME ####")
df = datasource0.toDF().toPandas()


#UPLOADING DATA
print(f"#### START UPLOAD DATA ####")
wr.s3.to_parquet(
    df=df,
    dataset=True,
    index = False,
    mode = 'overwrite',
    path=f's3://{bucket_prestage}/post-stage/{prefix}'
                )

print(f"#### THE DATA UPLOAD WAS ENDING ####")