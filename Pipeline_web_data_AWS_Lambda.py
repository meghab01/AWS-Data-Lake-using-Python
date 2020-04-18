3
from urllib import request
import boto3
import requests
from subprocess import call
def lambda_handler(event, context): 
    call('rm -rf /tmp/*', shell=True)
    url1='https://www.irs.gov/pub/irs-soi/eo1.csv'
    url2='https://www.irs.gov/pub/irs-soi/eo2.csv'
    url3='https://www.irs.gov/pub/irs-soi/eo3.csv'
    url4='https://www.irs.gov/pub/irs-soi/eo4.csv'
    u=[url1,url2,url3,url4]
    print(u)
    s3=boto3.client('s3')
    for url in u:
        r=''
        filename = url[32:]
        print(filename)
        print(url)
        r = requests.get(url)
        print("get complete")
        tmp='/tmp/'+str(filename)
        with open(tmp, 'wb') as f:
            f.write(r.content)
            f.close()
        print("temp lamda done")
        dest='People/External/Reference_Data/IRS_Data/'+str(filename)
        transfer = boto3.s3.transfer.S3Transfer(client=s3)
        transfer.upload_file(tmp,"bfas-developer-s3-swamp",dest,extra_args={'ServerSideEncryption':'aws:kms','SSEKMSKeyId':'alias/aws/s3'})
        print(filename+"upload complete")
        call('rm -rf /tmp/*', shell=True) 
        print("tmp empty")
    print("Done!!")