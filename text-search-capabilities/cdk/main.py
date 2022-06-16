#!/usr/bin/python
# Author: Borja Pérez Guasch <bpguasch@amazon.com>
# License: Apache 2.0
# Summary: module that creates the root stack of the application


from aws_cdk import (
    Stack,
    CfnParameter,
    Fn,
    Tags,
    aws_ssm as ssm
)

from constructs import Construct

from .data_source_indexation import DataSourceIndexationStack
from .data_source_analysis import DataSourceAnalysisStack
from .analysis_results_indexation import AnalysisResultsIndexationStack
from .global_ import GlobalResourcesStack

from assets.system_lambda_layer.python.language_analysis import constants
from assets.system_lambda_layer.python.language_analysis import tags


class LanguageAnalysisStack(Stack):
    __ANALYSIS_MODE_PARAM_DESC = 'The mode to use when running spaCy. By choosing Efficiency, \
the language analysis will be faster. If you choose Accuracy, the results will be more accurate but \
the analysis will take longer to complete.'
    __LANG_PARAM_DESC = 'Language of the data sources to analyse.'

    @property
    def stack_id_termination(self):
        return Fn.select(0, Fn.split('-', Fn.select(2, Fn.split('/', self.stack_id))))

    def __create_stack_parameters(self):
        analysis_mode = CfnParameter(self, 'analysisMode',
                                     default=constants.SPACY_MODE_EFFICIENCY,
                                     description=self.__ANALYSIS_MODE_PARAM_DESC,
                                     allowed_values=[constants.SPACY_MODE_ACCURACY,
                                                     constants.SPACY_MODE_EFFICIENCY],
                                     type='String')

        analysis_mode_ssm = ssm. \
            StringParameter(self, 'AnalysisModeParamSSM',
                            parameter_name=constants.CONFIG_PARAM_SPACY_MODE,
                            string_value=analysis_mode.value_as_string,
                            description=self.__ANALYSIS_MODE_PARAM_DESC)

        Tags.of(analysis_mode_ssm).add(tags.TAG_ENVIRONMENT, tags.CURRENT_ENVIRONMENT)
        Tags.of(analysis_mode_ssm).add(tags.TAG_MODULE, tags.MODULE_DATA_SOURCE_ANALYSIS)

        language = CfnParameter(self, 'language',
                                default='en',
                                description=self.__LANG_PARAM_DESC,
                                allowed_values=constants.SPACY_SUPPORTED_LANGUAGES,
                                type='String')

        language_ssm = ssm.StringParameter(self, 'LanguageParamSSM',
                                           parameter_name=constants.CONFIG_PARAM_LANGUAGE,
                                           string_value=language.value_as_string,
                                           description='{} It must be one of: {}.'.
                                           format(self.__LANG_PARAM_DESC,
                                                  ', '.join(constants.SPACY_SUPPORTED_LANGUAGES)))

        Tags.of(language_ssm).add(tags.TAG_ENVIRONMENT, tags.CURRENT_ENVIRONMENT)
        Tags.of(language_ssm).add(tags.TAG_MODULE, tags.MODULE_DATA_SOURCE_ANALYSIS)

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create configuration parameters
        self.__create_stack_parameters()

        # Nested stack with resources that are used by other stacks and that do not depend on any other resource
        self.global_resources_stack = GlobalResourcesStack(self, 'GlobalResources')

        # Nested stack that creates resources for validating data source files and indexing them
        self.indexation_stack = DataSourceIndexationStack(self, 'DataSourceIndexation')

        # Nested stack that creates the resources for analysing the data sources
        self.analysis_stack = DataSourceAnalysisStack(self, 'DataSourceAnalysis')

        # Nested stack that creates the resources for indexing the results of the language analysis
        self.analysis_results_stack = AnalysisResultsIndexationStack(self, 'AnalysisResultsIndexation')
