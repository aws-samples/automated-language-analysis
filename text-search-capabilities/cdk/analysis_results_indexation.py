#!/usr/bin/python
# Author: Borja Pérez Guasch <bpguasch@amazon.com>
# License: Apache 2.0
# Summary: module that creates a nested stack containing the resources for updating the OpenSearch cluster with the results of the language analysis


from aws_cdk import (
    RemovalPolicy,
    NestedStack,
    Tags,
    aws_iam as iam,
    Duration,
    aws_stepfunctions as step_functions,
    aws_stepfunctions_tasks as step_functions_tasks,
    aws_events as events,
    aws_events_targets as events_targets,
    aws_logs as logs,
    aws_lambda as lambda_
)

from constructs import Construct

from assets.system_lambda_layer.python.language_analysis import tags
from assets.system_lambda_layer.python.language_analysis import constants


class AnalysisResultsIndexationStack(NestedStack):
    def __create_analysis_results_indexation_lambda(self, layer, analysis_results_bucket, domain):
        # Create the log group so that it's cleaned when deleting the stack
        log_group = logs.LogGroup(self, 'IndexAnalysisResultsFunctionLogGroup',
                                  log_group_name='/aws/lambda/indexAnalysisResults',
                                  removal_policy=RemovalPolicy.DESTROY,
                                  retention=logs.RetentionDays.SIX_MONTHS)

        Tags.of(log_group).add(tags.TAG_ENVIRONMENT, tags.CURRENT_ENVIRONMENT)
        Tags.of(log_group).add(tags.TAG_MODULE, tags.MODULE_ANALYSIS_RESULTS_INDEXATION)

        function = lambda_.Function(self, 'AnalysisResultsIndexationFunction',
                                    function_name='indexAnalysisResults',
                                    handler='index.handler',
                                    runtime=lambda_.Runtime.PYTHON_3_9,
                                    timeout=Duration.minutes(15),
                                    code=lambda_.Code.from_asset('assets/func_index_analysis_results'),
                                    layers=[layer],
                                    retry_attempts=0,
                                    memory_size=1024)

        function.add_to_role_policy(
            iam.PolicyStatement(actions=['s3:GetObject'],
                                resources=[analysis_results_bucket.bucket_arn + '/*'])
        )

        function.add_to_role_policy(
            iam.PolicyStatement(actions=['ssm:GetParameter'],
                                resources=['arn:aws:ssm:*:{}:parameter/{}*'.format(self.account,
                                                                                   constants.SSM_PARAMS_PATH)])
        )

        function.node.add_dependency(log_group)

        Tags.of(function).add(tags.TAG_ENVIRONMENT, tags.CURRENT_ENVIRONMENT)
        Tags.of(function).add(tags.TAG_MODULE, tags.MODULE_ANALYSIS_RESULTS_INDEXATION)

        domain.grant_write(function)

        return function

    def __create_state_machine(self, indexation_function):
        succeeded_task = step_functions.Succeed(self, 'Indexation succeeded')
        indexation_fail_task = step_functions.Fail(self, 'Indexation failed')

        indexation_task = step_functions_tasks.LambdaInvoke(self, 'Index analysis results',
                                                            lambda_function=indexation_function,
                                                            output_path='$.Payload')
        indexation_task.next(succeeded_task)
        indexation_task.add_catch(handler=indexation_fail_task)

        state_machine = step_functions.StateMachine(self, 'AnalysisResultsIndexation',
                                                    state_machine_name='AnalysisResultsIndexation',
                                                    definition=indexation_task)

        Tags.of(state_machine).add(tags.TAG_ENVIRONMENT, tags.CURRENT_ENVIRONMENT)
        Tags.of(state_machine).add(tags.TAG_MODULE, tags.MODULE_ANALYSIS_RESULTS_INDEXATION)

        return state_machine

    def __create_state_machine_trigger_rule(self, bucket_to_listen, state_machine: step_functions.StateMachine):
        rule = events.Rule(self, 'DataSourceAnalysedRule',
                           rule_name='DataSourceAnalysedRule',
                           event_pattern=events.EventPattern(
                               source=['aws.s3'],
                               detail_type=['AWS API Call via CloudTrail'],
                               detail={
                                   'eventSource': ['s3.amazonaws.com'],
                                   'eventName': ['PutObject', 'CompleteMultipartUpload'],
                                   'requestParameters': {
                                       "bucketName": [bucket_to_listen.bucket_name]
                                   }
                               }
                           ),
                           targets=[
                               events_targets.SfnStateMachine(machine=state_machine)
                           ])

        Tags.of(rule).add(tags.TAG_ENVIRONMENT, tags.CURRENT_ENVIRONMENT)
        Tags.of(rule).add(tags.TAG_MODULE, tags.MODULE_ANALYSIS_RESULTS_INDEXATION)

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        layer = self.node.scope.global_resources_stack.layer
        domain = self.node.scope.global_resources_stack.opensearch_domain
        analysis_results_bucket = self.node.scope.analysis_stack.analysis_results_bucket

        function = self.__create_analysis_results_indexation_lambda(layer, analysis_results_bucket, domain)
        state_machine = self.__create_state_machine(function)

        self.__create_state_machine_trigger_rule(analysis_results_bucket, state_machine)
