#!/usr/bin/python
# Author: Borja Pérez Guasch <bpguasch@amazon.com>
# License: Apache 2.0
# Summary: script that performs several validations on a data source file and discards it if any of the validations fail


import json
import datetime
import boto3

from http import HTTPStatus

from language_analysis import constants
from language_analysis.utils import system_config, s3


__ERR_DATA_SOURCE_FILE_EXCEEDS_MAX_SIZE = 'The size of data source file {} ({} MB) exceeds the maximum allowed \
size of {} MB.'
__ERR_INVALID_DOCUMENT_FORMAT = 'Invalid document at line {}. Data source files must contain one JSON object per line \
with the fields {}'.format('{}', constants.REQUIRED_DOCUMENT_FIELDS)
__ERR_EMPTY_DATA_SOURCE_FILE = 'The data source file {} is empty.'
__ERR_MISSING_DOCUMENT_FIELD = 'Invalid document at line {}. Missing {} field.'
__ERR_EMPTY_DOCUMENT_FIELD = 'Invalid document at line {}. Field {} is empty.'
__ERR_INVALID_DOCUMENT_FIELD_TYPE = 'Invalid document at line {}. Field {} must be a string ({} found).'
__ERR_INVALID_DOCUMENT_DATE_FIELD_FORMAT = 'Invalid document at line {}. Field {} does not match format {}.'
__ERR_DATA_SOURCE_FILE_NOT_INSIDE_FOLDER = 'All data source files must be inside a folder with the name of the data \
source.'


class ValidationException(Exception):
    def __init__(self, message: str, status: int):
        super().__init__(message)
        self.status = status


def __validate_key(key: str) -> None:
    components = key.split('/')

    if len(components) == 1:
        raise ValidationException(status=HTTPStatus.UNPROCESSABLE_ENTITY,
                                  message=__ERR_DATA_SOURCE_FILE_NOT_INSIDE_FOLDER)


def __validate_data_source_file_size(file_size, file_name):
    # Divide to get the size in MB
    file_size /= 1000000

    if file_size == 0:
        raise ValidationException(message=__ERR_EMPTY_DATA_SOURCE_FILE.format(file_name), status=HTTPStatus.BAD_REQUEST)

    if file_size > constants.DATA_SOURCE_FILE_MAX_SIZE_MB:
        raise ValidationException(status=HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
                                  message=__ERR_DATA_SOURCE_FILE_EXCEEDS_MAX_SIZE.
                                  format(file_name, file_size, constants.DATA_SOURCE_FILE_MAX_SIZE_MB))


def __validate_data_source_file_format(lines):
    for i in range(len(lines)):
        try:
            document = json.loads(lines[i])
        except Exception:
            raise ValidationException(status=HTTPStatus.BAD_REQUEST,
                                      message=__ERR_INVALID_DOCUMENT_FORMAT.format(i + 1))

        # Verify that the document contains all the required fields
        __validate_document_fields(i + 1, document)


def __validate_document_fields(line_index, document):
    for field in constants.REQUIRED_DOCUMENT_FIELDS:
        # Verify that the document contains the field
        if field not in document:
            raise ValidationException(status=HTTPStatus.BAD_REQUEST,
                                      message=__ERR_MISSING_DOCUMENT_FIELD.format(line_index, field))

        # Verify that the field is a string
        if type(document[field]) != str:
            raise ValidationException(status=HTTPStatus.UNPROCESSABLE_ENTITY,
                                      message=__ERR_INVALID_DOCUMENT_FIELD_TYPE.
                                      format(line_index, field, document[field].__class__.__name__))

        # Verify that the field is not empty
        if not document[field]:
            raise ValidationException(status=HTTPStatus.UNPROCESSABLE_ENTITY,
                                      message=__ERR_EMPTY_DOCUMENT_FIELD.format(line_index, field))

    # Verify that the date field is properly formatted
    try:
        datetime.datetime.strptime(document[constants.DOCUMENT_FIELD_DATE], constants.DOCUMENT_FIELD_DATE_FORMAT)
    except ValueError:
        raise ValidationException(status=HTTPStatus.UNPROCESSABLE_ENTITY,
                                  message=__ERR_INVALID_DOCUMENT_DATE_FIELD_FORMAT.
                                  format(line_index,
                                         constants.DOCUMENT_FIELD_DATE,
                                         constants.DOCUMENT_FIELD_DATE_FORMAT))


def __move_invalid_file(source_bucket, destination_bucket_name, key):
    client = boto3.resource('s3')

    copy_source = {
        'Bucket': source_bucket,
        'Key': key
    }

    destination_bucket = client.Bucket(destination_bucket_name)
    destination_bucket.copy(copy_source, key)


def __delete_invalid_file(bucket, key):
    client = boto3.client('s3')
    client.delete_object(Bucket=bucket, Key=key)


def handler(event, context):
    bucket = event['detail']['requestParameters']['bucketName']
    key = event['detail']['requestParameters']['key']
    file_size = event['detail']['additionalEventData']['bytesTransferredIn']
    file_name = key.split('/')[-1]

    try:
        # Verify that the data source file is inside a folder in the bucket
        __validate_key(key)

        # Verify that the data source file is size is not 0 and does not exceed the maximum allowed
        __validate_data_source_file_size(file_size, file_name)

        # Verify that all the lines of the file contain a JSON object with all the required fields
        lines = s3.retrieve_file_contents(bucket, key).split('\n')
        __validate_data_source_file_format(lines)
    except ValidationException as e:
        # Retrieve the name of the invalid data sources bucket
        destination_bucket = system_config.get_parameter(constants.CONFIG_PARAM_INVALID_DATA_SOURCES_BUCKET)

        # Move the invalid file to the other bucket
        __move_invalid_file(bucket, destination_bucket, key)

        # Delete the invalid file from the data sources bucket
        __delete_invalid_file(bucket, key)

        raise e

    return {
        'statusCode': HTTPStatus.OK,
    }
