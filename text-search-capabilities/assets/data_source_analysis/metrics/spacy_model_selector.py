#!/usr/bin/python
# Author: Borja Pérez Guasch <bpguasch@amazon.com>
# License: Apache 2.0
# Summary: returns the name of the SpaCy model to use based on the language and analysis mode

import sys

mappings = {
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

lang = sys.argv[1]
mode = sys.argv[2]

print(mappings[lang][mode])
