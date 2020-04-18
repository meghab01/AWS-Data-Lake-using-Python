import boto3
import json
import pandas as pd
from subprocess import call
call('rm -rf /tmp/*', shell=True)
query='SELECT * FROM "bfas-developer-glue-database-swamp"."serde";'
database='bfas-developer-glue-database-swamp'
athena_client = boto3.client(service_name='athena', region_name='us-west-2')
response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database},
        ResultConfiguration={'OutputLocation': 's3://bfas-developer-s3-athena-results/People',
                              'EncryptionConfiguration': {'EncryptionOption': 'SSE_S3'}})
print('monitor downloaded')
s3=boto3.resource('s3')
s3.Bucket('bfas-developer-s3-athena-results').download_file('People/1c2d4564-47d3-4746-9c11-3065efaa5447.csv','/tmp/monitorfile.csv')
print('temp done')
j = pd.read_csv('/tmp/monitorfile.csv')
print(len(j))
ol=1
rows=[]
data=j.iloc[ol]['data'].split()
#Removing 'monitor_name',
row=['location','hr_from_source','date','time','uptime','status']
rows.append(row)
while(ol<len(j)):
#while(ol<5):
     location=j.iloc[ol]['locationname']
     hr_source=j.iloc[ol]['data_extracted_hour']
#     monitor=j.iloc[ol]['monitor']
     il=0
     while(il<len(data)-1):
#     while(il<5):
#        monitor_name=j['monitor_name'][ol]
#        time = j['data'][ol][il][1]
        date=data[il].lstrip('[')
        il+=1
        hour_mon=data[il].rstrip(',')
        il+=1
        uptime = data[il].rstrip(',')
        il+=1
        status = data[il].rstrip('],')
        il+=1
        #Removing monitor_name,
        row=[location,hr_source,date, hour_mon, uptime, status]
        rows.append(row)
        #print(row)
        #print('innerloop ',il)
     ol+=1
    #print('outerloop ',ol)
rowsdf=pd.DataFrame(rows)   
rowsdf.to_csv('\tmp\monitis_out.csv', sep=',', encoding='utf-8',index=False, header=None)

client = boto3.client('s3', 'us-west-2')
transfer = boto3.s3.transfer.S3Transfer(client=client)
transfer.upload_file('\tmp\monitis_out.csv','bfas-developer-s3-lake','Logs/Monitis/monitor_out.csv', extra_args={'ServerSideEncryption':"AES256"})
print('done') 


