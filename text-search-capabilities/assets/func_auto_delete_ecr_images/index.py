#!/usr/bin/python
# Author: Borja Pérez Guasch <bpguasch@amazon.com>
# License: Apache 2.0
# Summary: script that forces the deletion of a list of ECR repositories to prevent Stack deletion failure


import boto3
import cfnresponse


def handler(event, context):
    if event['RequestType'] == 'Delete':
        # Extract event variables
        repository_names = event['ResourceProperties']['repository_names']
        registry_id = event['ResourceProperties']['registry_id']

        client = boto3.client('ecr')

        for name in repository_names:
            client.delete_repository(
                registryId=registry_id,
                repositoryName=name,
                force=True
            )

    cfnresponse.send(event, context, cfnresponse.SUCCESS, {})
