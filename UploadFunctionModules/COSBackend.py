'''
Created on 6 mar. 2019

@author: David Fernandez
@author: Maite Bernaus
'''

import ibm_boto3
import ibm_botocore



class CosBackend:
    def __init__(self, args):
        client_config = ibm_botocore.client.Config(max_pool_connections=200)
      
   
        self.cos_client = ibm_boto3.client('s3',
                                           aws_access_key_id=args.get('ibm_cos_access_key'),
                                           aws_secret_access_key=args.get ('ibm_cos_secret_key'),
                                           config=client_config,
                                           endpoint_url=args.get('ibm_cos_endpoint'))
        
    def put_object(self, bucket_name,key,data):
        try:
            res = self.cos_client.put_object(Bucket=bucket_name, Key=key, Body=data)
            status='OK' if res['ResponseMetadata']['HTTPStatusCode']==200 else 'Error'
        except ibm_botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "NoSuchKey":
                raise e
            else:
                raise e
            
    def get_object(self, bucket_name, key, stream=False, extra_get_args={}):
        try: 
            r=self.cos_client.get_object(Bucket=bucket_name, Key=key, **extra_get_args)
            if stream:
                data=r['Body']
            else:
                data=r['Body'].read()
            return data
        except ibm_botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "NoSuchKey":
                raise e
            else:
                raise e
    
    def head_object(self, bucket_name, key):
        try:
            metadata=self.cos_client.head_object(Bucket=bucket_name, Key=key)
            return metadata['ResponseMetadata']['HTTPHeaders']
        except ibm_botocore.exceptions.ClientError as e:
            raise e
        
    def delete(self, bucket_name, key):
        return self.cos_client.delete_object(Bucket=bucket_name, Key=key)
    
    def get_buckets(self):
        print("Retrieving list of buckets")
        try:
            buckets = self.cos_client.buckets.all()
            for bucket in buckets:
                print("Bucket Name: {0}".format(bucket.name))
        except Exception as e:
            print("Unable to retrieve list buckets: {0}".format(e))
    
    def list_objects(self, bucket_name, prefix=None):
        paginator=self.cos_client.get_paginator('list_objects_v2')
        try:
            if (prefix is not None):
                page_iterator= paginator.paginate(Bucket=bucket_name, Prefix=prefix)
            else:
                page_iterator = paginator.paginate(Bucket=bucket_name)
            
            object_list = []
            for page in page_iterator:
                if 'Contents' in page:
                    for item in page['Contents']:
                        object_list.append(item)
            return object_list
        except ibm_botocore.exceptions.ClientError as e:
            raise e 