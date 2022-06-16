#!/usr/bin/python
# Author: Borja Pérez Guasch <bpguasch@amazon.com>
# License: Apache 2.0
# Summary: script that updates the OpenSearch domain indexes with the results of the language analysis


import json
import boto3
import os

from http import HTTPStatus
from opensearchpy import OpenSearch, RequestsHttpConnection, helpers, AWSV4SignerAuth
from language_analysis import constants
from language_analysis.utils import system_config, s3


ERRORS_FOLDER_NAME = '/errors/'


class IndexationException(Exception):
    def __init__(self, message: str, status: int):
        super().__init__(message)
        self.status = status


def __get_credentials(region):
    credentials = boto3.Session().get_credentials()
    return AWSV4SignerAuth(credentials, region)


def __get_domain(endpoint, auth):
    return OpenSearch(
        hosts=[{'host': endpoint, 'port': 443}],
        http_auth=auth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection
    )


def __generate_update_bulk_actions(documents: [dict], index: str) -> [dict]:
    actions = []

    for document in documents:
        actions.append({
            '_op_type': 'update',
            '_index': index,
            '_id': document['id'],
            'doc': document
        })

    return actions


def __generate_insert_bulk_actions(error_examples: [dict], index: str) -> [dict]:
    actions = []

    for error in error_examples:
        actions.append({
            '_op_type': 'index',
            '_index': index,
            '_type': 'language-error',
            '_id': error['id'],
            '_source': error
        })

    return actions


def handler(event, context):
    bucket = event['detail']['requestParameters']['bucketName']
    key = event['detail']['requestParameters']['key']

    # Generate an array with the documents by reading the contents of the file in S3
    documents = s3.retrieve_file_contents(bucket, key).split('\n')

    # Convert the documents to dictionaries
    documents = [json.loads(document) for document in documents]

    # Establish a connection with the Opensearch domain
    domain = __get_domain(system_config.get_parameter(constants.CONFIG_PARAM_OPENSEARCH_DOMAIN_ENDPOINT),
                          __get_credentials(os.environ['AWS_REGION']))

    # The examples of the language errors need to be indexed in the cluster
    if ERRORS_FOLDER_NAME in key:
        response = helpers.bulk(domain, __generate_insert_bulk_actions(documents, constants.INDEX_LANGUAGE_ERRORS))
    # The previously indexed documents need to be updated in the cluster with the analysis results
    else:
        response = helpers.bulk(domain, __generate_update_bulk_actions(documents, constants.INDEX_DOCUMENTS))

    # There were indexation errors
    if response[1]:
        raise IndexationException(message=json.dumps(response[1]), status=HTTPStatus.BAD_REQUEST)

    return {
        'statusCode': HTTPStatus.OK,
        'body': json.dumps({'indexedCount': response[0]})
    }
