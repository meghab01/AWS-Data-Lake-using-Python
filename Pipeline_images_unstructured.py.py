import os
import json
import boto3
import base64
from zipfile import ZipFile 
import io
from botocore.exceptions import ClientError
from subprocess import call

def get_secret():
    print("something")

    secret_name = "digitalocean"
    region_name = "us-west-2"

 # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)
    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
            j = json.loads(secret)
            aws_access_key_id = j['aws_access_key_id']
            aws_secret_access_key = j['aws_secret_access_key']
            return(j)
    else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
            print("password binary:" + decoded_binary_secret)
            aws_access_key_id = decoded_binary_secret.aws_access_key_id
            aws_secret_access_key = decoded_binary_secret.aws_secret_access_key
def handler(event, context):
    call('rm -rf /tmp/*', shell=True)
    k=get_secret()
    s3=boto3.resource('s3', 
                      aws_access_key_id=k['aws_access_key_id'],
                      aws_secret_access_key=k['aws_secret_access_key'],
                      region_name='sfo2',
                      endpoint_url='https://sfo2.digitaloceanspaces.com')
    s3bucket = os.environ['s3bucket']
    s3swamp = s3bucket+'-s3-swamp'                  
    prefix = 'com_craigslist/wv_huntington/all/2019-09-08/6974037219/'
    bucket = s3.Bucket(name="tech4pets-ginnie-us")
    FilesNotFound = True
    files=[]
    for obj in bucket.objects.filter(Prefix=prefix):
            key=obj.key[:]
            print(key)
           # key='com_craigslist/wv_huntington/all/2019-09-08/6974037219/aHR0cHM6Ly9pbWFnZXMuY3JhaWdzbGlzdC5vcmcvMDE0MTRfaG1EcVczZllLNV82MDB4NDUwLmpwZw=='
            list=key.split('/')
            if(len(list)<5):
                continue
            else:
                dest=('People/External/Pet_Sales_Craigslist/'+list[0]+'/'+list[1]+'/'+list[3][:4]+'/'+list[3][5:7]+'/'+list[3][8:10]+'/'+list[4]+'/')
                print(dest)
                file=list[5]+'.jpg'
                tmp='/tmp/'+str(file)
                print("Downloading: "+file)
                s3.Bucket('tech4pets-ginnie-us').download_file(key,tmp)
                s=boto3.client('s3')        
                destination=dest+str(file)
              # client = boto3.client('s3', 'us-west-2')
               # transfer = boto3.s3.transfer.S3Transfer(client=client)
                #transfer.upload_file(tmp,s3swamp,destination, extra_args={'ServerSideEncryption':'aws:kms', 'SSEKMSKeyId':'alias/aws/s3'})
                s3 = boto3.resource('s3')
                s3.Bucket(s3swamp).put_object(Key=destination,
                             Body=tmp,
                             ServerSideEncryption='aws:kms',
                             SSEKMSKeyId='alias/aws/s3')
                print(destination+" upload complete")
    print("Done!!")  
    call('rm -rf /tmp/*', shell=True)            



