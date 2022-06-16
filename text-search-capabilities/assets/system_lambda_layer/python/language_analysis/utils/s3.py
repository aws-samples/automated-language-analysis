#!/usr/bin/python
# Author: Borja Pérez Guasch <bpguasch@amazon.com>
# License: Apache 2.0
# Summary: module with helper methods to interact with S3

import boto3


def retrieve_file_contents(bucket: str, key: str) -> str:
    client = boto3.client('s3')
    response = client.get_object(Bucket=bucket, Key=key)
    return response['Body'].read().decode('utf-8')


def upload_contents(bucket: str, key: str, contents: str):
    client = boto3.client('s3')

    return client.put_object(
        Body=contents.encode('ascii'),
        Bucket=bucket,
        Key=key
    )
