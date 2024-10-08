from finance_complaint.constant.env_constant import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
import os
import boto3


class AWSConnectionConfig:

    s3_client = None
    s3_resource = None
    
    def __init__(self, region_name):

        if (AWSConnectionConfig.s3_resource == None) or (AWSConnectionConfig.s3_client == None):
            __access_key_id = AWS_ACCESS_KEY_ID
            __secret_access_key = AWS_SECRET_ACCESS_KEY
            
            if __access_key_id is None:
                raise Exception(f"Environment variable: AWS_ACCESS_KEY_ID is not set.")
            
            if __secret_access_key is None:
                raise Exception(f"Environment variable: AWS_SECRET_ACCESS_KEY is not set.")
        
            AWSConnectionConfig.s3_resource = boto3.resource('s3',
                                            aws_access_key_id=__access_key_id,
                                            aws_secret_access_key=__secret_access_key,
                                            region_name=region_name
                                            )
            
            AWSConnectionConfig.s3_client = boto3.client('s3',
                                        aws_access_key_id=__access_key_id,
                                        aws_secret_access_key=__secret_access_key,
                                        region_name=region_name
                                        )
            
        self.s3_resource = AWSConnectionConfig.s3_resource
        self.s3_client = AWSConnectionConfig.s3_client
        