#!/usr/bin/python
# Author: Borja Pérez Guasch <bpguasch@amazon.com>
# License: Apache 2.0
# Summary: module with helper methods to retrieve and set System Manager Parameter Store parameters


import boto3


def put_parameter(name: str, value):
    client = boto3.client('ssm')
    client.put_parameter(Name=name, Value=value)


def get_parameter(name: str):
    client = boto3.client('ssm')
    return client.get_parameter(Name=name)['Parameter']['Value']
