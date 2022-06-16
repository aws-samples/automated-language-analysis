#!/usr/bin/python
# Author: Joao Moura <joaopcm@amazon.com>, Borja Pérez Guasch <bpguasch@amazon.com>
# License: Apache 2.0
# Summary: script that calculates various language errors of the language used in the documents being processed

import sys
import boto3
import json
import os
import uuid
import language_tool_python as langtool


REGION = os.environ['AWS_REGION']

ANALYSIS_FOLDER_NAME = 'errors'
KEY_ID = 'id'
KEY_TEXT = 'text'

SSM_PARAMS_PATH = 'language-analysis'
CONFIG_PARAM_ANALYSIS_RESULTS_BUCKET = '/{}/analysisResultsBucket'.format(SSM_PARAMS_PATH)
CONFIG_PARAM_SPACY_MODE = '/{}/spaCyMode'.format(SSM_PARAMS_PATH)
CONFIG_PARAM_LANGUAGE = '/{}/language'.format(SSM_PARAMS_PATH)


def retrieve_file_contents(bucket: str, key: str) -> str:
    client = boto3.client('s3', region_name=REGION)
    response = client.get_object(Bucket=bucket, Key=key)
    return response['Body'].read().decode('utf-8')


def upload_contents(bucket: str, key: str, contents: str):
    client = boto3.client('s3', region_name=REGION)

    return client.put_object(
        Body=contents.encode('ascii'),
        Bucket=bucket,
        Key=key
    )


def generate_results_key(key: str) -> str:
    components = key.split('/')
    components.insert(-1, ANALYSIS_FOLDER_NAME)
    return '/'.join(components)


def get_parameter(name: str):
    client = boto3.client('ssm', region_name=REGION)
    return client.get_parameter(Name=name)['Parameter']['Value']


def analyse_document_text(checker, document: dict) -> [dict]:
    text = document[KEY_TEXT]
    matches = checker.check(text)

    return [
        {
            # Error specific fields
            'rule-id': match.ruleId,
            'category': match.category,
            'type': match.ruleIssueType,
            'context': match.context,
            'replacement': match.replacements[0] if match.replacements else '',
            KEY_ID: str(uuid.uuid4()),

            # Fields inherited from the document
            'country': document['country'],
            'country-code': document['country-code'],
            'date': document['date'],
            'document-id': document[KEY_ID],
            'source': document['source']
        } for match in matches]


if __name__ == '__main__':
    # Get from the command line arguments the name of the source bucket and the file that was uploaded
    indexed_data_sources_bucket = sys.argv[1]
    key = sys.argv[2]

    # Retrieve from SSM the values of some config parameters
    analysis_results_bucket = get_parameter(CONFIG_PARAM_ANALYSIS_RESULTS_BUCKET)
    language = get_parameter(CONFIG_PARAM_LANGUAGE)

    # Load the LanguageTool model to use
    checker = langtool.LanguageTool(language)

    # Retrieve the recently indexed documents and convert them to python dictionaries
    documents = retrieve_file_contents(indexed_data_sources_bucket, key).split('\n')
    documents = list(map(json.loads, documents))

    # Generate a list that contains the language errors found in the text of the document
    analysis_results = []

    for document in documents:
        analysis_results.extend(analyse_document_text(checker, document))

    # Only upload a results file if there are captured errors
    if analysis_results:
        # Generate a key that it's the same as the received one, but adding an extra folder in the last level
        results_key = generate_results_key(key)

        # Upload the results of the analysis
        upload_contents(analysis_results_bucket, results_key, '\n'.join([json.dumps(document)
                                                                         for document in analysis_results]))
