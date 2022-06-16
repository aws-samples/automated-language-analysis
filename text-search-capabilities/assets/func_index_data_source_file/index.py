#!/usr/bin/python
# Author: Borja Pérez Guasch <bpguasch@amazon.com>
# License: Apache 2.0
# Summary: script that indexes document files in the Opensearch domain


import json
import uuid
import boto3
import os

from http import HTTPStatus
from opensearchpy import OpenSearch, RequestsHttpConnection, helpers, AWSV4SignerAuth
from language_analysis import constants
from language_analysis.utils import system_config, s3


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


def __hydrate_document(document, data_source, key_id) -> dict:
    new_keys = {'source': data_source, 'id': key_id}
    return {**document, **new_keys}


def __generate_bulk_actions(documents: [dict], index: str) -> [dict]:
    actions = []

    for document in documents:
        actions.append({
            '_op_type': 'index',
            '_index': index,
            '_type': 'document',
            '_id': document['id'],
            '_source': document
        })

    return actions


def handler(event, context):
    bucket = event['detail']['requestParameters']['bucketName']
    key = event['detail']['requestParameters']['key']
    data_source = key.split('/')[0]

    # Generate an array with the documents by reading the contents of the file in S3
    documents = s3.retrieve_file_contents(bucket, key).split('\n')

    # Convert the documents to dictionaries and add to them some fields
    documents = [__hydrate_document(json.loads(document),
                                    data_source,
                                    str(uuid.uuid4())) for document in documents]

    # Establish a connection with the Opensearch domain
    domain = __get_domain(system_config.get_parameter(constants.CONFIG_PARAM_OPENSEARCH_DOMAIN_ENDPOINT),
                          __get_credentials(os.environ['AWS_REGION']))

    # Send the request
    response = helpers.bulk(domain, __generate_bulk_actions(documents, constants.INDEX_DOCUMENTS))

    # There were indexation errors
    if response[1]:
        raise IndexationException(message=json.dumps(response[1]), status=HTTPStatus.BAD_REQUEST)

    # Upload the indexed documents to S3
    contents = '\n'.join([json.dumps(document) for document in documents])
    indexed_data_sources_bucket = system_config.get_parameter(constants.CONFIG_PARAM_INDEXED_DATA_SOURCES_BUCKET)
    s3.upload_contents(indexed_data_sources_bucket, key, contents)

    return {
        'statusCode': HTTPStatus.OK,
        'body': json.dumps({'indexedCount': response[0]})
    }
