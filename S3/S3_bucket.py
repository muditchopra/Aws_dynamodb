import boto3
from botocore.exceptions import ClientError
import logging

class Bucket:

    def create_bucket(self,bucket_name, region=None):
        try:
            if not region:
                self.s3_client = boto3.client('s3')
                self.response = self.s3_client.create_bucket(Bucket=bucket_name)
            else:
                self.s3_client = boto3.client('s3', region_name=region)
                location = {'LocationConstraint': region}
                self.response = self.s3_client.create_bucket(Bucket=bucket_name,
                                        CreateBucketConfiguration=location)
                # self.response = self.s3_client.create_bucket(Bucket=bucket_name)
        except ClientError as e:
            logging.error(e)
            return False
        return self.response['ResponseMetadata']['HTTPStatusCode']

    def bucket_list(self,s3=None):
        if not s3:
            self.s3 = boto3.client('s3')
        else:
            self.s3 = s3
        self.response = self.s3.list_buckets()
        return self.response['Buckets']

    def upload_file(self,file_name, bucket, object_name=None,s3=None):

        if object_name is None:
            self.object_name = file_name
            self.file_name = file_name
            self.bucket = bucket
        else:
            self.object_name = object_name
            self.file_name = file_name
            self.bucket = bucket

        if not s3:
            self.s3_client = boto3.client('s3')
        else:
            self.s3 = s3
        try:
            self.response = self.s3_client.upload_file(self.file_name, self.bucket, self.object_name)
        except ClientError as e:
            logging.error(e)
            return False
        return True



if __name__ =='__main__':

    obj = Bucket()
    # res = obj.create_bucket('test-lambda-trigger123','us-east-2')
    # print(res)

    response = obj.bucket_list()
    # Output the bucket names
    print('Existing buckets:')
    for bucket in response:
        print(f'  {bucket["Name"]}')

    #upload file to bucket 'test-lambda-trigger123'

    response = obj.upload_file('panda.png','test-lambda-trigger123')
    print(response)
