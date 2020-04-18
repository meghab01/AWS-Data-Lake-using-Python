import os
import json
import boto3
import base64
from zipfile import ZipFile 
import io
from botocore.exceptions import ClientError
from subprocess import call
#new version to QA
def get_secret():
    secret_name = "digitalocean"
    region_name = "us-west-2"
 # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)
    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    if 'SecretString' in get_secret_value_response:
            print("inside if")
            secret = get_secret_value_response['SecretString']
            j = json.loads(secret)
            print(j)
            #aws_access_key_id = j['doaccesskeyid']
            #aws_secret_access_key = j['doaccesskeyid']
            return(j)
    else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
            print("password binary:" + decoded_binary_secret)
            aws_access_key_id = decoded_binary_secret.doaccesskeyid
            aws_secret_access_key = decoded_binary_secret.doaccesskeyid
            return(decoded_binary_secret)
            
def handler(event, context):
    call('rm -rf /tmp/*', shell=True)
    k=get_secret()
    s3swamp=os.environ['s3bucket']
    s3=boto3.resource('s3', 
                  aws_access_key_id=k['doaccesskeyid'],
                  aws_secret_access_key=k['dosecretkey'],
                  region_name='sfo2',
                  endpoint_url='https://sfo2.digitaloceanspaces.com')
                  
    prefix = 'monthly_extracts/'
    bucket = s3.Bucket(name="tech4pets-ginnie-us")
    FilesNotFound = True
    files=[]
    for obj in bucket.objects.filter(Prefix=prefix):
        f=obj.key[17:]
        if f !="":
            files.append(f)
        FilesNotFound = False
    print(files)
    if FilesNotFound:
        print("ALERT", "No file in {0}/{1}".format(bucket, prefix))
    for file in files:
        file_src='monthly_extracts/'+str(file)
        file_name='pet_sales/'+str(file)
        tmp='/tmp/'+str(file)
        print("Downloading: "+file)
        s3.Bucket('tech4pets-ginnie-us').download_file(file_src,tmp)
        with ZipFile(tmp, 'r') as zip: 
    # printing all the contents of the zip file 
            zip.printdir() 
    # extracting all the files 
            print('Extracting all the files now...') 
            zip.extractall('/tmp/')  
            destdir=[]
            for f in zip.filelist:
                destdir.append(f.filename)
        print(destdir)
        s=boto3.client('s3')        
        for f in destdir:
            tmp='/tmp/'+str(f)
            dest='People/External/Pet_Sales_Craigslist/'+'year='+file[23:-7]+'/'+'month='+file[28:-4]+'/'+str(f)
            #s = boto3.client('s3')
            #s.put_object(Bucket = s3swamp, Key=dest, Body=tmp, ServerSideEncryption="AES256")
            client = boto3.client('s3', 'us-west-2')
            transfer = boto3.s3.transfer.S3Transfer(client=client)
            transfer.upload_file(tmp, s3swamp, dest, extra_args={'ServerSideEncryption':"AES256"})
            print(dest+" upload complete")
        call('rm -rf /tmp/*', shell=True)    
    print("Done!!")  
    call('rm -rf /tmp/*', shell=True)
    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
