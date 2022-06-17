#!/usr/bin/python
# Author: Borja Pérez Guasch <bpguasch@amazon.com>
# License: Apache 2.0
# Summary: module that creates a nested stack containing the resources for analysing the data sources


from aws_cdk import (
    RemovalPolicy,
    NestedStack,
    aws_s3 as s3,
    Tags,
    aws_ssm as ssm,
    aws_cloudtrail as cloudtrail,
    aws_codecommit as codecommit,
    aws_codebuild as codebuild,
    aws_iam as iam,
    aws_ecr as ecr,
    aws_ecs as ecs,
    aws_batch_alpha as batch,
    Duration,
    aws_stepfunctions as step_functions,
    aws_stepfunctions_tasks as step_functions_tasks,
    aws_events as events,
    aws_events_targets as events_targets,
)

from constructs import Construct

from assets.system_lambda_layer.python.language_analysis import tags
from assets.system_lambda_layer.python.language_analysis import constants
from cdk.code_commit_to_ecr_pipeline import CodeCommitToECRPipeline, CodeCommitToECRPipelineProps


class DataSourceAnalysisStack(NestedStack):
    __COMMAND_GET_LANG = "LANG=$(aws ssm get-parameter --name /language-analysis/language | jq -r '.Parameter.Value')"
    __COMMAND_GET_SPACY_MODE = "SPACY_MODE=$(aws ssm get-parameter --name /language-analysis/spaCyMode | \
jq -r '.Parameter.Value')"
    __COMMAND_GET_SPACY_MODEL = 'SPACY_MODEL=$(python3 spacy_model_selector.py $LANG $SPACY_MODE)'
    __COMMAND_BUILD = 'docker build -t $ECR_REPO_NAME:$IMAGE_TAG --build-arg SPACY_MODEL=$SPACY_MODEL .'

    def __create_s3_bucket(self) -> s3.Bucket:
        bucket = s3.Bucket(self, 'AnalysisResultsBucket',
                           bucket_name='analysis-results-' + self.node.scope.stack_id_termination,
                           removal_policy=RemovalPolicy.DESTROY,
                           auto_delete_objects=True)

        Tags.of(bucket).add(tags.TAG_ENVIRONMENT, tags.CURRENT_ENVIRONMENT)
        Tags.of(bucket).add(tags.TAG_MODULE, tags.MODULE_DATA_SOURCE_ANALYSIS)

        analysis_results_bucket_ssm = ssm. \
            StringParameter(self, 'AnalysisResultsBucketSSM',
                            parameter_name=constants.CONFIG_PARAM_ANALYSIS_RESULTS_BUCKET,
                            string_value=bucket.bucket_name)

        Tags.of(analysis_results_bucket_ssm).add(tags.TAG_ENVIRONMENT, tags.CURRENT_ENVIRONMENT)
        Tags.of(analysis_results_bucket_ssm).add(tags.TAG_MODULE, tags.MODULE_DATA_SOURCE_ANALYSIS)

        return bucket

    def __create_s3_object_level_events_trail(self, bucket):
        trail_bucket = s3.Bucket(self, 'AnalysisS3ObjectLevelEventsTrailBucket',
                                 auto_delete_objects=True,
                                 removal_policy=RemovalPolicy.DESTROY)

        Tags.of(trail_bucket).add(tags.TAG_ENVIRONMENT, tags.CURRENT_ENVIRONMENT)
        Tags.of(trail_bucket).add(tags.TAG_MODULE, tags.MODULE_DATA_SOURCE_ANALYSIS)

        trail = cloudtrail.Trail(self, 'AnalysisS3ObjectLevelEventsTrail',
                                 trail_name='data-source-analysis-s3-object-level-events',
                                 is_multi_region_trail=True,
                                 bucket=trail_bucket)

        trail.add_s3_event_selector(
            s3_selector=[
                cloudtrail.S3EventSelector(bucket=bucket)
            ],
            include_management_events=False,
            read_write_type=cloudtrail.ReadWriteType.WRITE_ONLY
        )

        Tags.of(trail).add(tags.TAG_ENVIRONMENT, tags.CURRENT_ENVIRONMENT)
        Tags.of(trail).add(tags.TAG_MODULE, tags.MODULE_DATA_SOURCE_ANALYSIS)

    def __create_metrics_dev_tools(self):
        # Retrieve the default buildspec of the module
        buildspec = CodeCommitToECRPipeline.buildspec()
        build_commands = buildspec['phases']['build']['commands']

        # Add additional commands at the beginning and modify the Docker build command
        build_commands = [self.__COMMAND_GET_LANG,
                          self.__COMMAND_GET_SPACY_MODE,
                          self.__COMMAND_GET_SPACY_MODEL] + build_commands
        build_commands[3] = self.__COMMAND_BUILD

        # Update the commands of the build phase
        buildspec['phases']['build']['commands'] = build_commands

        props = CodeCommitToECRPipelineProps(
            ecr_repository_props=ecr.RepositoryProps(
                removal_policy=RemovalPolicy.DESTROY,
                repository_name='language-analysis/metrics'),

            codecommit_repository_props=codecommit.RepositoryProps(
                repository_name='language-analysis-metrics',
                code=codecommit.Code.from_directory('assets/data_source_analysis/metrics')),

            codebuild_project_props=codebuild.PipelineProjectProps(
                build_spec=buildspec
            )
        )

        pipeline = CodeCommitToECRPipeline(self, 'MetricsPipeline', props)
        pipeline.codebuild_project.add_to_role_policy(
            iam.PolicyStatement(
                actions=['ssm:GetParameter'],
                resources=['arn:aws:ssm:*:{}:parameter/{}*'.format(self.account, constants.SSM_PARAMS_PATH)])
        )
        pipeline.ecr_repository.add_lifecycle_rule(description='Expire untagged images after 1 day.',
                                                   max_image_age=Duration.days(1),
                                                   tag_status=ecr.TagStatus.UNTAGGED)

        Tags.of(pipeline).add(tags.TAG_ENVIRONMENT, tags.CURRENT_ENVIRONMENT)
        Tags.of(pipeline).add(tags.TAG_MODULE, tags.MODULE_DATA_SOURCE_ANALYSIS)

        return pipeline

    def __create_errors_dev_tools(self):
        # Retrieve the default buildspec of the module
        buildspec = CodeCommitToECRPipeline.buildspec()
        build_commands = buildspec['phases']['build']['commands']

        # Add additional commands at the beginning and modify the Docker build command
        build_commands = [self.__COMMAND_GET_LANG,
                          self.__COMMAND_GET_SPACY_MODE,
                          self.__COMMAND_GET_SPACY_MODEL] + build_commands
        build_commands[3] = self.__COMMAND_BUILD

        # Update the commands of the build phase
        buildspec['phases']['build']['commands'] = build_commands

        props = CodeCommitToECRPipelineProps(
            ecr_repository_props=ecr.RepositoryProps(
                removal_policy=RemovalPolicy.DESTROY,
                repository_name='language-analysis/errors'),

            codecommit_repository_props=codecommit.RepositoryProps(
                repository_name='language-analysis-errors',
                code=codecommit.Code.from_directory('assets/data_source_analysis/errors')),

            codebuild_project_props=codebuild.PipelineProjectProps(
                build_spec=buildspec
            )
        )

        pipeline = CodeCommitToECRPipeline(self, 'ErrorsPipeline', props)
        pipeline.codebuild_project.add_to_role_policy(
            iam.PolicyStatement(
                actions=['ssm:GetParameter'],
                resources=['arn:aws:ssm:*:{}:parameter/{}*'.format(self.account, constants.SSM_PARAMS_PATH)])
        )
        pipeline.ecr_repository.add_lifecycle_rule(description='Expire untagged images after 1 day.',
                                                   max_image_age=Duration.days(1),
                                                   tag_status=ecr.TagStatus.UNTAGGED)

        Tags.of(pipeline).add(tags.TAG_ENVIRONMENT, tags.CURRENT_ENVIRONMENT)
        Tags.of(pipeline).add(tags.TAG_MODULE, tags.MODULE_DATA_SOURCE_ANALYSIS)

        return pipeline

    def __create_metrics_aws_batch_components(self, ecr_repository: ecr.Repository, vpc, indexed_data_sources_bucket,
                                              analysis_results_bucket, config_files_bucket):
        spot_ce = batch.ComputeEnvironment(self, 'MetricsSpotCE',
                                           compute_environment_name='Metrics-Spot-CE',
                                           compute_resources=batch.ComputeResources(
                                               type=batch.ComputeResourceType.SPOT,
                                               allocation_strategy=batch.AllocationStrategy.SPOT_CAPACITY_OPTIMIZED,
                                               minv_cpus=0,
                                               maxv_cpus=128,
                                               vpc=vpc
                                           ))

        Tags.of(spot_ce).add(tags.TAG_ENVIRONMENT, tags.CURRENT_ENVIRONMENT)
        Tags.of(spot_ce).add(tags.TAG_MODULE, tags.MODULE_DATA_SOURCE_ANALYSIS)

        on_demand_ce = batch.ComputeEnvironment(self, 'MetricsOnDemandCE',
                                                compute_environment_name='Metrics-OnDemand-CE',
                                                compute_resources=batch.ComputeResources(
                                                    type=batch.ComputeResourceType.ON_DEMAND,
                                                    allocation_strategy=batch.AllocationStrategy.BEST_FIT_PROGRESSIVE,
                                                    minv_cpus=0,
                                                    maxv_cpus=32,
                                                    vpc=vpc
                                                ))

        Tags.of(on_demand_ce).add(tags.TAG_ENVIRONMENT, tags.CURRENT_ENVIRONMENT)
        Tags.of(on_demand_ce).add(tags.TAG_MODULE, tags.MODULE_DATA_SOURCE_ANALYSIS)

        compute_environments: [batch.JobQueueComputeEnvironment] = [
            batch.JobQueueComputeEnvironment(order=1, compute_environment=on_demand_ce),
            batch.JobQueueComputeEnvironment(order=2, compute_environment=spot_ce)
        ]

        queue = batch.JobQueue(self, 'MetricsQueue',
                               job_queue_name='Metrics-Queue',
                               compute_environments=compute_environments)

        Tags.of(queue).add(tags.TAG_ENVIRONMENT, tags.CURRENT_ENVIRONMENT)
        Tags.of(queue).add(tags.TAG_MODULE, tags.MODULE_DATA_SOURCE_ANALYSIS)

        role = iam.Role(self, 'MetricsAWSBatchRole',
                        assumed_by=iam.ServicePrincipal(service='ecs-tasks.amazonaws.com'),
                        managed_policies=[
                            iam.ManagedPolicy.from_managed_policy_arn(self, 'MetricsAmazonEC2ContainerRegistryReadOnly',
                                                                      'arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly'),
                            iam.ManagedPolicy.from_managed_policy_arn(self, 'MetricsAmazonEC2ContainerServiceforEC2Role',
                                                                      'arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceforEC2Role')
                        ],
                        inline_policies={
                            'S3Read': iam.PolicyDocument(statements=[
                                iam.PolicyStatement(effect=iam.Effect.ALLOW,
                                                    actions=['s3:GetObject', 's3:PutObject'],
                                                    resources=[indexed_data_sources_bucket.bucket_arn + '/*',
                                                               analysis_results_bucket.bucket_arn + '/*',
                                                               config_files_bucket.bucket_arn + '/*'])
                                ]),
                            'SSMGet': iam.PolicyDocument(statements=[
                                iam.PolicyStatement(effect=iam.Effect.ALLOW,
                                                    actions=['ssm:GetParameter'],
                                                    resources=['arn:aws:ssm:*:{}:parameter/{}*'.format(self.account, constants.SSM_PARAMS_PATH)])
                            ])
                        })

        Tags.of(role).add(tags.TAG_ENVIRONMENT, tags.CURRENT_ENVIRONMENT)
        Tags.of(role).add(tags.TAG_MODULE, tags.MODULE_DATA_SOURCE_ANALYSIS)

        definition = batch.JobDefinition(self, 'MetricsJobDefinition',
                                         job_definition_name='Metrics-Job-Definition',
                                         retry_attempts=1,
                                         container=batch.JobDefinitionContainer(
                                             environment={'AWS_REGION': NestedStack.of(self).region},
                                             vcpus=2,
                                             memory_limit_mib=4096,
                                             execution_role=role,
                                             job_role=role,
                                             image=ecs.EcrImage(ecr_repository, "latest"),
                                             command=['Ref::indexed_data_sources_bucket',
                                                      'Ref::key']
                                         ))

        Tags.of(definition).add(tags.TAG_ENVIRONMENT, tags.CURRENT_ENVIRONMENT)
        Tags.of(definition).add(tags.TAG_MODULE, tags.MODULE_DATA_SOURCE_ANALYSIS)

        return queue, definition

    def __create_errors_aws_batch_components(self, ecr_repository: ecr.Repository, vpc, indexed_data_sources_bucket,
                                             analysis_results_bucket):
        spot_ce = batch.ComputeEnvironment(self, 'ErrorsSpotCE',
                                           compute_environment_name='Errors-Spot-CE',
                                           compute_resources=batch.ComputeResources(
                                               type=batch.ComputeResourceType.SPOT,
                                               allocation_strategy=batch.AllocationStrategy.SPOT_CAPACITY_OPTIMIZED,
                                               minv_cpus=0,
                                               maxv_cpus=32,
                                               vpc=vpc
                                           ))

        Tags.of(spot_ce).add(tags.TAG_ENVIRONMENT, tags.CURRENT_ENVIRONMENT)
        Tags.of(spot_ce).add(tags.TAG_MODULE, tags.MODULE_DATA_SOURCE_ANALYSIS)

        on_demand_ce = batch.ComputeEnvironment(self, 'ErrorsOnDemandCE',
                                                compute_environment_name='Errors-OnDemand-CE',
                                                compute_resources=batch.ComputeResources(
                                                    type=batch.ComputeResourceType.ON_DEMAND,
                                                    allocation_strategy=batch.AllocationStrategy.BEST_FIT_PROGRESSIVE,
                                                    minv_cpus=0,
                                                    maxv_cpus=16,
                                                    vpc=vpc
                                                ))

        Tags.of(on_demand_ce).add(tags.TAG_ENVIRONMENT, tags.CURRENT_ENVIRONMENT)
        Tags.of(on_demand_ce).add(tags.TAG_MODULE, tags.MODULE_DATA_SOURCE_ANALYSIS)

        compute_environments: [batch.JobQueueComputeEnvironment] = [
            batch.JobQueueComputeEnvironment(order=1, compute_environment=on_demand_ce),
            batch.JobQueueComputeEnvironment(order=2, compute_environment=spot_ce)
        ]

        queue = batch.JobQueue(self, 'ErrorsQueue',
                               job_queue_name='Errors-Queue',
                               compute_environments=compute_environments)

        Tags.of(queue).add(tags.TAG_ENVIRONMENT, tags.CURRENT_ENVIRONMENT)
        Tags.of(queue).add(tags.TAG_MODULE, tags.MODULE_DATA_SOURCE_ANALYSIS)

        role = iam.Role(self, 'ErrorsAWSBatchRole',
                        assumed_by=iam.ServicePrincipal(service='ecs-tasks.amazonaws.com'),
                        managed_policies=[
                            iam.ManagedPolicy.from_managed_policy_arn(self, 'ErrorsAmazonEC2ContainerRegistryReadOnly',
                                                                      'arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly'),
                            iam.ManagedPolicy.from_managed_policy_arn(self, 'ErrorsAmazonEC2ContainerServiceforEC2Role',
                                                                      'arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceforEC2Role')
                        ],
                        inline_policies={
                            'S3Read': iam.PolicyDocument(statements=[
                                iam.PolicyStatement(effect=iam.Effect.ALLOW,
                                                    actions=['s3:GetObject', 's3:PutObject'],
                                                    resources=[indexed_data_sources_bucket.bucket_arn + '/*',
                                                               analysis_results_bucket.bucket_arn + '/*'])
                                ]),
                            'SSMGet': iam.PolicyDocument(statements=[
                                iam.PolicyStatement(effect=iam.Effect.ALLOW,
                                                    actions=['ssm:GetParameter'],
                                                    resources=['arn:aws:ssm:*:{}:parameter/{}*'.format(self.account, constants.SSM_PARAMS_PATH)])
                            ])
                        })

        Tags.of(role).add(tags.TAG_ENVIRONMENT, tags.CURRENT_ENVIRONMENT)
        Tags.of(role).add(tags.TAG_MODULE, tags.MODULE_DATA_SOURCE_ANALYSIS)

        definition = batch.JobDefinition(self, 'ErrorsJobDefinition',
                                         job_definition_name='Errors-Job-Definition',
                                         retry_attempts=1,
                                         container=batch.JobDefinitionContainer(
                                             environment={'AWS_REGION': NestedStack.of(self).region},
                                             vcpus=2,
                                             memory_limit_mib=4096,
                                             execution_role=role,
                                             job_role=role,
                                             image=ecs.EcrImage(ecr_repository, "latest"),
                                             command=['Ref::indexed_data_sources_bucket',
                                                      'Ref::key']
                                         ))

        Tags.of(definition).add(tags.TAG_ENVIRONMENT, tags.CURRENT_ENVIRONMENT)
        Tags.of(definition).add(tags.TAG_MODULE, tags.MODULE_DATA_SOURCE_ANALYSIS)

        return queue, definition

    def __create_state_machine(self, metrics_job_queue, metrics_job_definition,
                               errors_job_queue, errors_job_definition):
        submit_metrics_job = step_functions_tasks.BatchSubmitJob(self, 'Submit metrics calculation job',
                                                                 job_name='MetricsCalculation',
                                                                 job_queue_arn=metrics_job_queue.job_queue_arn,
                                                                 job_definition_arn=metrics_job_definition.job_definition_arn,
                                                                 payload=step_functions.TaskInput.from_object(
                                                                     {
                                                                         "indexed_data_sources_bucket": step_functions.JsonPath.string_at("$.detail.requestParameters.bucketName"),
                                                                         "key": step_functions.JsonPath.string_at("$.detail.requestParameters.key")
                                                                     }
                                                                 ))

        submit_errors_job = step_functions_tasks.BatchSubmitJob(self, 'Submit errors calculation job',
                                                                job_name='ErrorsCalculation',
                                                                job_queue_arn=errors_job_queue.job_queue_arn,
                                                                job_definition_arn=errors_job_definition.job_definition_arn,
                                                                payload=step_functions.TaskInput.from_object(
                                                                    {
                                                                        "indexed_data_sources_bucket": step_functions.JsonPath.string_at(
                                                                            "$.detail.requestParameters.bucketName"),
                                                                        "key": step_functions.JsonPath.string_at(
                                                                            "$.detail.requestParameters.key")
                                                                    }
                                                                ))

        map_task = step_functions.Parallel(self, 'Run analysis in parallel')\
            .branch(submit_metrics_job)\
            .branch(submit_errors_job)

        state_machine = step_functions.StateMachine(self, 'DataSourceAnalysis',
                                                    state_machine_name='DataSourceAnalysis',
                                                    definition=map_task)
        state_machine.add_to_role_policy(iam.PolicyStatement(effect=iam.Effect.ALLOW,
                                                             actions=['batch:SubmitJob'],
                                                             resources=[metrics_job_queue.job_queue_arn,
                                                                        metrics_job_definition.job_definition_arn,
                                                                        errors_job_queue.job_queue_arn,
                                                                        errors_job_definition.job_definition_arn
                                                                        ]))

        Tags.of(state_machine).add(tags.TAG_ENVIRONMENT, tags.CURRENT_ENVIRONMENT)
        Tags.of(state_machine).add(tags.TAG_MODULE, tags.MODULE_DATA_SOURCE_ANALYSIS)

        return state_machine

    def __create_state_machine_trigger_rule(self, bucket_to_listen, state_machine: step_functions.StateMachine):
        rule = events.Rule(self, 'DataSourceIndexedRule',
                           rule_name='DataSourceIndexedRule',
                           event_pattern=events.EventPattern(
                               source=['aws.s3'],
                               detail_type=['AWS API Call via CloudTrail'],
                               detail={
                                   'eventSource': ['s3.amazonaws.com'],
                                   'eventName': ['PutObject', 'CompleteMultipartUpload'],
                                   'requestParameters': {
                                       "bucketName": [bucket_to_listen.bucket_name],
                                   }
                               }
                           ),
                           targets=[
                               events_targets.SfnStateMachine(machine=state_machine)
                           ])

        Tags.of(rule).add(tags.TAG_ENVIRONMENT, tags.CURRENT_ENVIRONMENT)
        Tags.of(rule).add(tags.TAG_MODULE, tags.MODULE_DATA_SOURCE_ANALYSIS)

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        vpc = self.node.scope.global_resources_stack.vpc
        indexed_data_sources_bucket = self.node.scope.indexation_stack.indexed_data_sources_bucket
        config_files_bucket = self.node.scope.global_resources_stack.config_files_bucket

        self.analysis_results_bucket = self.__create_s3_bucket()
        self.__create_s3_object_level_events_trail(self.analysis_results_bucket)

        metrics_pipeline = self.__create_metrics_dev_tools()

        metrics_queue, metrics_definition = self.__create_metrics_aws_batch_components(
            metrics_pipeline.ecr_repository,
            vpc,
            indexed_data_sources_bucket,
            self.analysis_results_bucket,
            config_files_bucket)

        errors_pipeline = self.__create_errors_dev_tools()

        errors_queue, errors_definition = self.__create_errors_aws_batch_components(
            errors_pipeline.ecr_repository,
            vpc,
            indexed_data_sources_bucket,
            self.analysis_results_bucket)

        state_machine = self.__create_state_machine(metrics_queue, metrics_definition, errors_queue, errors_definition)
        self.__create_state_machine_trigger_rule(indexed_data_sources_bucket, state_machine)
