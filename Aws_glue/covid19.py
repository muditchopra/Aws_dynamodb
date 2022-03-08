import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col
from awsglue.job import Job


sc = SparkContext()
glueContext = GlueContext(sc)
saprk = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'],args)

db_name = 'covid1-data-db'
tb_name = 'read'
s3_write_path = 's3://covid1-data/write'


datasource = glueContext.create_dynamic_frame.from_catalog(database=db_name,table_name=tb_name)

dataframe = datasource.toDF()

dataframe01 = dataframe.drop('last update')

corona_df = dataframe01.dropna(thresh=4)

cleared_df = corona_df.fillna(value = 'na_province_state', subset='province/state')

most_cases_province_state_df = cleansed_data_df.groupBy('province/state', 'country/region').max('confirmed')\
    .select('province/state', 'country/region', col("max(confirmed)").alias("Most_Cases"))\
    .orderBy('max(confirmed)', ascending=False)

most_deaths_province_state_df = cleansed_data_df.groupBy('province/state', 'country/region').max('deaths')\
    .select('province/state', 'country/region', col("max(deaths)").alias("Most_Deaths"))\
    .orderBy('max(deaths)', ascending=False)

most_recoveries_province_state_df = cleansed_data_df.groupBy('province/state', 'country/region').max('recovered')\
    .select('province/state', 'country/region', col("max(recovered)").alias("Most_Recovered"))\
    .orderBy('max(recovered)', ascending=False)


transform1 = DynamicFrame.fromDF(most_cases_province_state_df, glueContext, 'transform1')
transform2 = DynamicFrame.fromDF(most_deaths_province_state_df, glueContext, 'transform2')
transform3 = DynamicFrame.fromDF(most_recoveries_province_state_df, glueContext, 'transform3')

datasink1 = glueContext.write_dynamic_frame.from_options(frame=transform1, connection_type="s3", connection_options={
                                                         "path": s3_write_bucket+'/most-cases'}, format="parquet", transformation_ctx="datasink1")
datasink2 = glueContext.write_dynamic_frame.from_options(frame=transform2, connection_type="s3", connection_options={
                                                         "path": s3_write_bucket+'/most-deaths'}, format="parquet", transformation_ctx="datasink2")
datasink3 = glueContext.write_dynamic_frame.from_options(frame=transform3, connection_type="s3", connection_options={
                                                         "path": s3_write_bucket+'/most-recoveries'}, format="parquet", transformation_ctx="datasink3")

job.commit()