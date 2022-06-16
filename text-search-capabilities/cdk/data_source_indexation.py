#!/usr/bin/python
# Author: Borja Pérez Guasch <bpguasch@amazon.com>
# License: Apache 2.0
# Summary: module that creates a nested stack containing the resources for validating and indexing data source files


from aws_cdk import (
    RemovalPolicy,
    NestedStack,
    aws_s3 as s3,
    Tags,
    aws_lambda as lambda_,
    Duration,
    aws_iam as iam,
    aws_stepfunctions as step_functions,
    aws_stepfunctions_tasks as step_functions_tasks,
    aws_events as events,
    aws_events_targets as events_targets,
    aws_logs as logs,
    aws_ssm as ssm,
    aws_cloudtrail as cloudtrail,
)

from constructs import Construct

from assets.system_lambda_layer.python.language_analysis import constants
from assets.system_lambda_layer.python.language_analysis import tags


class DataSourceIndexationStack(NestedStack):
    def __create_invalid_data_sources_bucket(self):
        bucket = s3.Bucket(self, 'InvalidDataSourcesBucket',
                           bucket_name='invalid-data-sources-' + self.node.scope.stack_id_termination,
                           removal_policy=RemovalPolicy.DESTROY,
                           auto_delete_objects=True)

        Tags.of(bucket).add(tags.TAG_ENVIRONMENT, tags.CURRENT_ENVIRONMENT)
        Tags.of(bucket).add(tags.TAG_MODULE, tags.MODULE_DATA_SOURCE_INDEXATION)

        invalid_data_sources_bucket_ssm = ssm. \
            StringParameter(self, 'InvalidDataSourcesBucketSSM',
                            parameter_name=constants.CONFIG_PARAM_INVALID_DATA_SOURCES_BUCKET,
                            string_value=bucket.bucket_name)

        Tags.of(invalid_data_sources_bucket_ssm).add(tags.TAG_ENVIRONMENT, tags.CURRENT_ENVIRONMENT)
        Tags.of(invalid_data_sources_bucket_ssm).add(tags.TAG_MODULE, tags.MODULE_DATA_SOURCE_INDEXATION)

        return bucket

    def __create_data_sources_bucket(self) -> s3.Bucket:
        bucket = s3.Bucket(self, 'DataSourcesBucket',
                           bucket_name='data-sources-' + self.node.scope.stack_id_termination,
                           removal_policy=RemovalPolicy.DESTROY,
                           auto_delete_objects=True)

        Tags.of(bucket).add(tags.TAG_ENVIRONMENT, tags.CURRENT_ENVIRONMENT)
        Tags.of(bucket).add(tags.TAG_MODULE, tags.MODULE_DATA_SOURCE_INDEXATION)

        data_sources_bucket_ssm = ssm. \
            StringParameter(self, 'DataSourcesBucketSSM',
                            parameter_name=constants.CONFIG_PARAM_DATA_SOURCES_BUCKET,
                            string_value=bucket.bucket_name)

        Tags.of(data_sources_bucket_ssm).add(tags.TAG_ENVIRONMENT, tags.CURRENT_ENVIRONMENT)
        Tags.of(data_sources_bucket_ssm).add(tags.TAG_MODULE, tags.MODULE_DATA_SOURCE_INDEXATION)

        return bucket

    def __create_indexed_data_sources_bucket(self) -> s3.Bucket:
        bucket = s3.Bucket(self, 'IndexedDataSourcesBucket',
                           bucket_name='indexed-data-sources-' + self.node.scope.stack_id_termination,
                           removal_policy=RemovalPolicy.DESTROY,
                           auto_delete_objects=True)

        Tags.of(bucket).add(tags.TAG_ENVIRONMENT, tags.CURRENT_ENVIRONMENT)
        Tags.of(bucket).add(tags.TAG_MODULE, tags.MODULE_DATA_SOURCE_INDEXATION)

        indexed_data_sources_bucket_ssm = ssm. \
            StringParameter(self, 'IndexedDataSourcesBucketSSM',
                            parameter_name=constants.CONFIG_PARAM_INDEXED_DATA_SOURCES_BUCKET,
                            string_value=bucket.bucket_name)

        Tags.of(indexed_data_sources_bucket_ssm).add(tags.TAG_ENVIRONMENT, tags.CURRENT_ENVIRONMENT)
        Tags.of(indexed_data_sources_bucket_ssm).add(tags.TAG_MODULE, tags.MODULE_DATA_SOURCE_INDEXATION)

        return bucket

    def __create_s3_object_level_events_trail(self, data_sources_bucket, indexed_data_sources_bucket):
        trail_bucket = s3.Bucket(self, 'IndexationS3ObjectLevelEventsTrailBucket',
                                 auto_delete_objects=True,
                                 removal_policy=RemovalPolicy.DESTROY)

        Tags.of(trail_bucket).add(tags.TAG_ENVIRONMENT, tags.CURRENT_ENVIRONMENT)
        Tags.of(trail_bucket).add(tags.TAG_MODULE, tags.MODULE_DATA_SOURCE_INDEXATION)

        trail = cloudtrail.Trail(self, 'IndexationS3ObjectLevelEventsTrail',
                                 trail_name='data-source-indexation-s3-object-level-events',
                                 is_multi_region_trail=True,
                                 bucket=trail_bucket)

        trail.add_s3_event_selector(
            s3_selector=[
                cloudtrail.S3EventSelector(bucket=data_sources_bucket),
                cloudtrail.S3EventSelector(bucket=indexed_data_sources_bucket)
            ],
            include_management_events=False,
            read_write_type=cloudtrail.ReadWriteType.WRITE_ONLY
        )

        Tags.of(trail).add(tags.TAG_ENVIRONMENT, tags.CURRENT_ENVIRONMENT)
        Tags.of(trail).add(tags.TAG_MODULE, tags.MODULE_DATA_SOURCE_INDEXATION)

    def __create_data_source_file_validation_lambda(self, layer, data_sources_bucket, invalid_data_sources_bucket):
        # Create the log group so that it's cleaned when deleting the stack
        log_group = logs.LogGroup(self, 'DataSourceFileValidatorFunctionLogGroup',
                                  log_group_name='/aws/lambda/validateDataSourceFile',
                                  removal_policy=RemovalPolicy.DESTROY,
                                  retention=logs.RetentionDays.SIX_MONTHS)

        Tags.of(log_group).add(tags.TAG_ENVIRONMENT, tags.CURRENT_ENVIRONMENT)
        Tags.of(log_group).add(tags.TAG_MODULE, tags.MODULE_DATA_SOURCE_INDEXATION)

        function = lambda_.Function(self, 'DataSourceFileValidatorFunction',
                                    function_name='validateDataSourceFile',
                                    handler='index.handler',
                                    runtime=lambda_.Runtime.PYTHON_3_9,
                                    timeout=Duration.minutes(15),
                                    code=lambda_.Code.from_asset('assets/func_validate_data_source_file'),
                                    layers=[layer],
                                    retry_attempts=0,
                                    memory_size=1024)

        function.add_to_role_policy(
            iam.PolicyStatement(actions=['s3:GetObject', 's3:DeleteObject'],
                                resources=[data_sources_bucket.bucket_arn + '/*'])
        )

        function.add_to_role_policy(
            iam.PolicyStatement(actions=['s3:*'],
                                resources=[invalid_data_sources_bucket.bucket_arn + '/*'])
        )

        function.add_to_role_policy(
            iam.PolicyStatement(actions=['ssm:GetParameter'],
                                resources=['arn:aws:ssm:*:{}:parameter/{}*'.format(self.account,
                                                                                   constants.SSM_PARAMS_PATH)])
        )

        function.node.add_dependency(log_group)

        Tags.of(function).add(tags.TAG_ENVIRONMENT, tags.CURRENT_ENVIRONMENT)
        Tags.of(function).add(tags.TAG_MODULE, tags.MODULE_DATA_SOURCE_INDEXATION)

        return function

    def __create_data_source_file_indexation_lambda(self, layer, data_sources_bucket, indexed_data_sources_bucket,
                                                    domain):
        # Create the log group so that it's cleaned when deleting the stack
        log_group = logs.LogGroup(self, 'DataSourceFileIndexationFunctionLogGroup',
                                  log_group_name='/aws/lambda/indexDataSourceFile',
                                  removal_policy=RemovalPolicy.DESTROY,
                                  retention=logs.RetentionDays.SIX_MONTHS)

        Tags.of(log_group).add(tags.TAG_ENVIRONMENT, tags.CURRENT_ENVIRONMENT)
        Tags.of(log_group).add(tags.TAG_MODULE, tags.MODULE_DATA_SOURCE_INDEXATION)

        function = lambda_.Function(self, 'DataSourceFileIndexationFunction',
                                    function_name='indexDataSourceFile',
                                    handler='index.handler',
                                    runtime=lambda_.Runtime.PYTHON_3_9,
                                    timeout=Duration.minutes(15),
                                    code=lambda_.Code.from_asset('assets/func_index_data_source_file'),
                                    layers=[layer],
                                    retry_attempts=0,
                                    memory_size=1024)

        function.add_to_role_policy(
            iam.PolicyStatement(actions=['s3:GetObject'],
                                resources=[data_sources_bucket.bucket_arn + '/*'])
        )

        function.add_to_role_policy(
            iam.PolicyStatement(actions=['s3:PutObject'],
                                resources=[indexed_data_sources_bucket.bucket_arn + '/*'])
        )

        function.add_to_role_policy(
            iam.PolicyStatement(actions=['ssm:GetParameter'],
                                resources=['arn:aws:ssm:*:{}:parameter/{}*'.format(self.account,
                                                                                   constants.SSM_PARAMS_PATH)])
        )

        function.node.add_dependency(log_group)

        Tags.of(function).add(tags.TAG_ENVIRONMENT, tags.CURRENT_ENVIRONMENT)
        Tags.of(function).add(tags.TAG_MODULE, tags.MODULE_DATA_SOURCE_INDEXATION)

        domain.grant_write(function)

        return function

    def __create_state_machine(self, validation_function, indexation_function):
        validation_fail_task = step_functions.Fail(self, 'Validation failed')
        succeeded_task = step_functions.Succeed(self, 'Indexation succeeded')
        indexation_fail_task = step_functions.Fail(self, 'Indexation failed')

        indexation_task = step_functions_tasks.LambdaInvoke(self, 'Index data source file',
                                                            lambda_function=indexation_function,
                                                            output_path='$.Payload')
        indexation_task.next(succeeded_task)
        indexation_task.add_catch(handler=indexation_fail_task)

        validation_task = step_functions_tasks.LambdaInvoke(self, 'Validate data source file',
                                                            lambda_function=validation_function,
                                                            result_path=step_functions.JsonPath.DISCARD)

        validation_task.add_catch(handler=validation_fail_task)
        validation_task.next(indexation_task)

        state_machine = step_functions.StateMachine(self, 'DataSourceIndexation',
                                                    state_machine_name='DataSourceIndexation',
                                                    definition=validation_task)

        Tags.of(state_machine).add(tags.TAG_ENVIRONMENT, tags.CURRENT_ENVIRONMENT)
        Tags.of(state_machine).add(tags.TAG_MODULE, tags.MODULE_DATA_SOURCE_INDEXATION)

        return state_machine

    def __create_state_machine_trigger_rule(self, bucket_to_listen, state_machine: step_functions.StateMachine):
        rule = events.Rule(self, 'DataSourceUploadedRule',
                           rule_name='DataSourceUploadedRule',
                           event_pattern=events.EventPattern(
                               source=['aws.s3'],
                               detail_type=['AWS API Call via CloudTrail'],
                               detail={
                                   'eventSource': ['s3.amazonaws.com'],
                                   'eventName': ['PutObject', 'CompleteMultipartUpload'],
                                   'requestParameters': {
                                       "bucketName": [bucket_to_listen.bucket_name],
                                       "x-amz-storage-class": [{
                                           "exists": True
                                       }]
                                   }
                               }
                           ),
                           targets=[
                               events_targets.SfnStateMachine(machine=state_machine)
                           ])

        Tags.of(rule).add(tags.TAG_ENVIRONMENT, tags.CURRENT_ENVIRONMENT)
        Tags.of(rule).add(tags.TAG_MODULE, tags.MODULE_DATA_SOURCE_INDEXATION)

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        layer = self.node.scope.global_resources_stack.layer
        domain = self.node.scope.global_resources_stack.opensearch_domain

        invalid_data_sources_bucket = self.__create_invalid_data_sources_bucket()
        data_sources_bucket = self.__create_data_sources_bucket()
        self.indexed_data_sources_bucket = self.__create_indexed_data_sources_bucket()

        self.__create_s3_object_level_events_trail(data_sources_bucket, self.indexed_data_sources_bucket)

        validation_function = self.__create_data_source_file_validation_lambda(layer,
                                                                               data_sources_bucket,
                                                                               invalid_data_sources_bucket)

        indexation_function = self.__create_data_source_file_indexation_lambda(layer,
                                                                               data_sources_bucket,
                                                                               self.indexed_data_sources_bucket,
                                                                               domain)

        state_machine = self.__create_state_machine(validation_function, indexation_function)
        self.__create_state_machine_trigger_rule(data_sources_bucket, state_machine)
