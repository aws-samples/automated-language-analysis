#!/usr/bin/python
# Author: Borja Pérez Guasch <bpguasch@amazon.com>
# License: Apache 2.0
# Summary: module with several constant values used in the deployment of the CDK project


# -------------------- DATA SOURCES -------------------- #
REQUIRED_DOCUMENT_FIELDS = [
    'country-code',
    'country',
    'date',
    'text'
]

DOCUMENT_FIELD_COUNTRY_CODE = REQUIRED_DOCUMENT_FIELDS[0]
DOCUMENT_FIELD_COUNTRY = REQUIRED_DOCUMENT_FIELDS[1]
DOCUMENT_FIELD_DATE = REQUIRED_DOCUMENT_FIELDS[2]
DOCUMENT_FIELD_TEXT = REQUIRED_DOCUMENT_FIELDS[3]

DOCUMENT_FIELD_DATE_FORMAT = '%Y-%m-%d'

DATA_SOURCE_FILE_MAX_SIZE_MB = 50

# ------------------- SYSTEM CONFIG -------------------- #
SSM_PARAMS_PATH = 'language-analysis'

CONFIG_PARAM_SPACY_MODE = '/{}/spaCyMode'.format(SSM_PARAMS_PATH)
CONFIG_PARAM_DATA_SOURCES_BUCKET = '/{}/dataSourcesBucket'.format(SSM_PARAMS_PATH)
CONFIG_PARAM_INVALID_DATA_SOURCES_BUCKET = '/{}/invalidDataSourcesBucket'.format(SSM_PARAMS_PATH)
CONFIG_PARAM_INDEXED_DATA_SOURCES_BUCKET = '/{}/indexedDataSourcesBucket'.format(SSM_PARAMS_PATH)
CONFIG_PARAM_ANALYSIS_RESULTS_BUCKET = '/{}/analysisResultsBucket'.format(SSM_PARAMS_PATH)
CONFIG_PARAM_CONFIG_FILES_BUCKET = '/{}/configFilesBucket'.format(SSM_PARAMS_PATH)
CONFIG_PARAM_FOREIGNISMS = '/{}/foreignisms'.format(SSM_PARAMS_PATH)
CONFIG_PARAM_LANGUAGE = '/{}/language'.format(SSM_PARAMS_PATH)
CONFIG_PARAM_OPENSEARCH_DOMAIN_ENDPOINT = '/{}/opensearchDomainEndpoint'.format(SSM_PARAMS_PATH)

# ----------------------- SPACY ------------------------ #
SPACY_MODE_ACCURACY = 'Accuracy'
SPACY_MODE_EFFICIENCY = 'Efficiency'
SPACY_SUPPORTED_LANGUAGES = ['ca', 'zh', 'da', 'nl', 'en', 'fr', 'de', 'el', 'it',
                             'ja', 'pl', 'pt', 'ro', 'ru', 'es']

# -------------------- OPENSEARCH ---------------------- #
INDEX_DOCUMENTS = 'documents'
INDEX_LANGUAGE_ERRORS = 'language-errors'
