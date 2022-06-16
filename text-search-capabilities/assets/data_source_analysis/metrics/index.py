#!/usr/bin/python
# Author: Paolo Romeo <paolorom@amazon.com>, Borja Pérez Guasch <bpguasch@amazon.com>
# License: Apache 2.0
# Summary: script that calculates various metrics of the language used in the documents being processed

import sys
import boto3
import spacy
import json
import string
import warnings
import os

from lexical_diversity import lex_div as ld

spacy_models = {
    'ca': {
        'Efficiency': 'ca_core_news_sm',
        'Accuracy': 'ca_core_news_trf'
    },
    'zh': {
        'Efficiency': 'zh_core_web_sm',
        'Accuracy': 'zh_core_web_trf'
    },
    'da': {
        'Efficiency': 'da_core_news_sm',
        'Accuracy': 'da_core_news_trf'
    },
    'nl': {
        'Efficiency': 'nl_core_news_sm',
        'Accuracy': 'nl_core_news_lg'
    },
    'en': {
        'Efficiency': 'en_core_web_sm',
        'Accuracy': 'en_core_web_trf'
    },
    'fr': {
        'Efficiency': 'fr_core_news_sm',
        'Accuracy': 'fr_dep_news_trf'
    },
    'de': {
        'Efficiency': 'de_core_news_sm',
        'Accuracy': 'de_dep_news_trf'
    },
    'el': {
        'Efficiency': 'el_core_news_sm',
        'Accuracy': 'el_core_news_lg'
    },
    'it': {
        'Efficiency': 'it_core_news_sm',
        'Accuracy': 'it_core_news_lg'
    },
    'ja': {
        'Efficiency': 'ja_core_news_sm',
        'Accuracy': 'ja_core_news_trf'
    },
    'lt': {
        'Efficiency': 'lt_core_news_sm',
        'Accuracy': 'lt_core_news_lg'
    },
    'mk': {
        'Efficiency': 'mk_core_news_sm',
        'Accuracy': 'mk_core_news_lg'
    },
    'xx': {
        'Efficiency': 'xx_ent_wiki_sm',
        'Accuracy': 'xx_sent_ud_sm'
    },
    'nb': {
        'Efficiency': 'nb_core_news_sm',
        'Accuracy': 'nb_core_news_lg'
    },
    'pl': {
        'Efficiency': 'pl_core_news_sm',
        'Accuracy': 'pl_core_news_lg'
    },
    'pt': {
        'Efficiency': 'pt_core_news_sm',
        'Accuracy': 'pt_core_news_lg'
    },
    'ro': {
        'Efficiency': 'ro_core_news_sm',
        'Accuracy': 'ro_core_news_lg'
    },
    'ru': {
        'Efficiency': 'ru_core_news_sm',
        'Accuracy': 'ru_core_news_lg'
    },
    'es': {
        'Efficiency': 'es_core_news_sm',
        'Accuracy': 'es_dep_news_trf'
    }
}

warnings.simplefilter(action='ignore')

REGION = os.environ['AWS_REGION']

ANALYSIS_FOLDER_NAME = 'metrics'
KEY_ID = 'id'
KEY_TEXT = 'text'
FOREIGNISMS_FILE_NAME = 'foreignisms.txt'

SSM_PARAMS_PATH = 'language-analysis'
CONFIG_PARAM_ANALYSIS_RESULTS_BUCKET = '/{}/analysisResultsBucket'.format(SSM_PARAMS_PATH)
CONFIG_PARAM_CONFIG_FILES_BUCKET = '/{}/configFilesBucket'.format(SSM_PARAMS_PATH)
CONFIG_PARAM_SPACY_MODE = '/{}/spaCyMode'.format(SSM_PARAMS_PATH)
CONFIG_PARAM_LANGUAGE = '/{}/language'.format(SSM_PARAMS_PATH)


def retrieve_file_contents(bucket: str, key: str) -> str:
    client = boto3.client('s3', region_name=REGION)
    response = client.get_object(Bucket=bucket, Key=key)
    return response['Body'].read().decode('utf-8')


def retrieve_foreignisms() -> [str]:
    foreignisms = retrieve_file_contents(system_config_bucket, FOREIGNISMS_FILE_NAME).split('\n')
    return [' {} '.format(word.strip()) for word in foreignisms]


def upload_contents(bucket: str, key: str, contents: str):
    client = boto3.client('s3', region_name=REGION)

    return client.put_object(
        Body=contents.encode('ascii'),
        Bucket=bucket,
        Key=key
    )


def build_translation_table():
    """
    Builds a translation used to remove punctuation while searching for foreignisms in text
    :return: translation table
    """
    table = {}

    for i in string.punctuation:
        if i != '-':
            table[i] = ' '

    for i in string.digits:
        table[i] = ' '

    table['¿'] = ' '
    table['¡'] = ' '

    return str.maketrans(table)


def find_foreignisms_in_text(text, translation_table, foreignisms):
    # Adding blank spaces to allow for recognition of foreignisms in first and last position
    processed_text = ' {} '.format(text.lower().translate(translation_table))
    found_foreignisms = []

    # Count how many times foreign words appear in text
    for word in foreignisms:
        if word in processed_text:
            found_foreignisms.extend([word.strip() for _ in range(processed_text.count(word))])

    return found_foreignisms


def analyse_document_text(nlp, foreignisms, translation_table, text: str) -> dict:
    tokens = nlp(text)

    # Part of speech analysis
    adjectives = [x.lower_ for x in tokens if x.pos_ == 'ADJ']
    lemm_adjectives = [x.lemma_.lower() for x in tokens if x.pos_ == 'ADJ']
    unique_lemm_adjectives = len(set(lemm_adjectives))

    nouns = [x.lower_ for x in tokens if x.pos_ == 'NOUN']
    lemm_nouns = [x.lemma_.lower() for x in tokens if x.pos_ == 'NOUN']
    unique_lemm_nouns = len(set(lemm_nouns))

    verbs = [x.lower_ for x in tokens if x.pos_ == 'VERB']
    lemm_verbs = [x.lemma_.lower() for x in tokens if x.pos_ == 'VERB']
    unique_lemm_verbs = len(set(lemm_verbs))

    adverbs = [x.lower_ for x in tokens if x.pos_ == 'ADV']

    # General metrics
    tokens = [x.lower_ for x in tokens]

    ttr = ld.ttr(tokens)
    mtld = ld.mtld(tokens)
    n_tokens = len(tokens)

    # Part of speech percentage analysis

    # Adjectives
    adj_pct = round(len(adjectives) / n_tokens * 100, 2)
    unique_lemm_adj_pct = round(unique_lemm_adjectives / n_tokens * 100, 2)

    # Nouns
    nouns_pct = round(len(nouns) / n_tokens * 100, 2)
    unique_lemm_nouns_pct = round(unique_lemm_nouns / n_tokens * 100, 2)

    # Verbs
    verbs_pct = round(len(verbs) / n_tokens * 100, 2)
    unique_lemm_verbs_pct = round(unique_lemm_verbs / n_tokens * 100, 2)

    # Adverbs
    adverbs_pct = round(len(adverbs) / n_tokens * 100, 2)
    unique_adverbs_pct = round(len(set(adverbs)) / n_tokens * 100, 2)

    # Foreignisms
    if foreignisms:
        fw_list = find_foreignisms_in_text(text, translation_table, foreignisms)
        fw_pct = round(len(fw_list) / n_tokens * 100, 2)
    else:
        fw_list = []
        fw_pct = 0

    return {
        'ttr': ttr,
        'mtld': mtld,
        'tokens': n_tokens,
        'adj_pct': adj_pct,
        'unique_lemm_adj_pct': unique_lemm_adj_pct,
        'lemm_adjectives': lemm_adjectives,
        'nouns_pct': nouns_pct,
        'unique_lemm_nouns_pct': unique_lemm_nouns_pct,
        'lemm_nouns': lemm_nouns,
        'verbs_pct': verbs_pct,
        'unique_lemm_verbs_pct': unique_lemm_verbs_pct,
        'lemm_verbs': lemm_verbs,
        'adverbs_pct': adverbs_pct,
        'unique_adverbs_pct': unique_adverbs_pct,
        'adverbs': adverbs,
        'fw_pct': fw_pct,
        'fw_list': fw_list
    }


def generate_results_key(key: str) -> str:
    components = key.split('/')
    components.insert(-1, ANALYSIS_FOLDER_NAME)
    return '/'.join(components)


def get_parameter(name: str):
    client = boto3.client('ssm', region_name=REGION)
    return client.get_parameter(Name=name)['Parameter']['Value']


if __name__ == '__main__':
    # Get from the command line arguments the name of the source bucket and the file that was uploaded
    indexed_data_sources_bucket = sys.argv[1]
    key = sys.argv[2]

    # Retrieve from SSM the values of some config parameters
    analysis_results_bucket = get_parameter(CONFIG_PARAM_ANALYSIS_RESULTS_BUCKET)
    system_config_bucket = get_parameter(CONFIG_PARAM_CONFIG_FILES_BUCKET)
    mode = get_parameter(CONFIG_PARAM_SPACY_MODE)
    language = get_parameter(CONFIG_PARAM_LANGUAGE)

    # Determine the SpaCy model to use based on the chosen language and analysis mode
    nlp = spacy.load(spacy_models[language][mode])

    # Retrieve the recently indexed documents and convert them to python dictionaries
    documents = retrieve_file_contents(indexed_data_sources_bucket, key).split('\n')
    documents = list(map(json.loads, documents))

    # Retrieve the list of foreignisms to detect
    foreignisms = retrieve_foreignisms()

    # Build a translation table to remove punctuation
    translation_table = build_translation_table()

    # Generate a list that contains the identifier of the document and the calculated data points
    analysis_results = [
        {
            **{KEY_ID: document[KEY_ID]},
            **analyse_document_text(nlp, foreignisms, translation_table, document[KEY_TEXT])
        } for document in documents
    ]

    # Generate a key that it's the same as the received one, but adding an extra folder in the last level
    results_key = generate_results_key(key)

    # Upload the results of the analysis
    upload_contents(analysis_results_bucket, results_key, '\n'.join([json.dumps(document)
                                                                     for document in analysis_results]))
