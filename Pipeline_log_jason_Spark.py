import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.types import StringType, ArrayType, DataType
from pyspark.sql import SparkSession
from awsglue.job import Job
import time, datetime
import boto3
import time, datetime
import json
import pandas
# Setting the Spark Session and Initialize the GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
now = str(datetime.date.today() + datetime.timedelta(days=1))
now_minus_1 = str(datetime.date.today() - datetime.timedelta(days=90))
last_1_predicate = "data_extracted_timestamp between '" + now_minus_1 + "' AND '" + now + "'"
monitis_intake_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
	database = 'bfas-sandbox-glue-database-lake',
	table_name = 'serde',
#	transformation_ctx = 'monitis_intake_dynamic_frame',
	push_down_predicate = last_1_predicate)
# Converts the DynamicFrame to a Spark Compatible DataFrame
monitis_intake_data_frame = monitis_intake_dynamic_frame.toDF()
print(monitis_intake_data_frame.count())

#If there are no files loaded in last 1 day
if monitis_intake_data_frame is None or monitis_intake_data_frame.count() == 0:
	print('No monitor files to process')
	sys.exit()
else:
	pass
#select * from <> where datatimestamp not in select distinct datatimestamp from 
j=monitis_intake_data_frame.toPandas()

ol=0
rows=[]
row=['monitor_name','location','datetime','time','status','data_extracted_year','data_extracted_month','data_extracted_day','data_extracted_hour','data_extracted_timestamp']
rows.append(row)
while(ol<len(j)):
    il=0
    while(il<len(j['data'][ol])-1):
        monitor_name=j['monitor_name'][ol]
        location=j['locationName'][ol]
        datetime = j['data'][ol][il][0].string
        time = j['data'][ol][il][1].string
        status = j['data'][ol][il][2].string
        data_extracted_year=j['data_extracted_year'][ol]
        data_extracted_month=j['data_extracted_month'][ol]
        data_extracted_day=j['data_extracted_day'][ol]
        data_extracted_hour=j['data_extracted_hour'][ol]
        data_extracted_timestamp=j['data_extracted_timestamp'][ol]
        row=[monitor_name,location,datetime,time,status,data_extracted_year,data_extracted_month,data_extracted_day,data_extracted_hour,data_extracted_timestamp]
        rows.append(row)
        #print(row)
        il+=1
        #print('innerloop ',il)
    ol+=1
    #print('outerloop ',ol)
df = sc.parallelize(rows).toDF(['monitor_name','location','datetime','time','status','data_extracted_year','data_extracted_month','data_extracted_day','data_extracted_hour','data_extracted_timestamp'])
df.createOrReplaceTempView("df_sql")
#Convert the data frame back to a dynamic frame
final_dynamic_frame = DynamicFrame.fromDF(df,glueContext,"final_dynamic_frame")

# Write the joined dynamic frame out to a datasink
datasink = glueContext.write_dynamic_frame.from_options(frame = final_dynamic_frame, connection_type = "s3", connection_options = {'path': 's3://bfas-sandbox-s3-lake/Logs/Monitis'}, format = "parquet", transformation_ctx = "datasink")
print('done')
audit_data_frame=spark.sql("select distinct data_extracted_timestamp as dataExtractedTimestamp, monitor_name as ecosystemName from df_sql")
audit_data_frame.show(5)
# Create the boto3 resource
dynamodb = boto3.resource('dynamodb','us-west-2')
print('dynamodb initialized')
# Define the table name using the argument
table = dynamodb.Table('bfas-sandbox-dynamodb-configs-enterprise-data-shelterluv-etl-audit')
# Write the item to DynamoDB
audit_data_frame_json = audit_data_frame.toJSON()
try:
    for row in audit_data_frame_json.collect():
    #json string
        print(row)
        table.put_item( Item = json.loads(row) )
        print('Audit trail successfully')
except Exception as e:
	print(e)
	print('Could not write data to the Audit Table')
	pass
#Print Completion Statement
print('ETL Job is complete')
#print('End Time: ' + str(datetime.datetime.now()))

# Commit the job!
#job.commit()


