#!/usr/bin/python
# Author: Borja Pérez Guasch <bpguasch@amazon.com>
# License: Apache 2.0
# Summary: module that creates a nested stack containing resources used by other stacks


from aws_cdk import (
    RemovalPolicy,
    NestedStack,
    aws_lambda as lambda_,
    Tags,
    aws_opensearchservice as opensearch,
    aws_ssm as ssm,
    aws_s3 as s3,
    aws_s3_deployment as s3_deployment,
    aws_ec2 as ec2
)

from constructs import Construct

from assets.system_lambda_layer.python.language_analysis import tags
from assets.system_lambda_layer.python.language_analysis import constants


class GlobalResourcesStack(NestedStack):
    __LAYER_DESC = 'Package with helper methods and constant values that are common to the different scripts.'

    def __create_lambda_layer(self):
        layer = lambda_.LayerVersion(self, 'SystemLayer',
                                     layer_version_name='SystemLayer',
                                     compatible_architectures=[lambda_.Architecture.X86_64,
                                                               lambda_.Architecture.ARM_64],
                                     compatible_runtimes=[lambda_.Runtime.PYTHON_3_7,
                                                          lambda_.Runtime.PYTHON_3_9],
                                     removal_policy=RemovalPolicy.DESTROY,
                                     code=lambda_.AssetCode('assets/system_lambda_layer'),
                                     description=self.__LAYER_DESC)

        Tags.of(layer).add(tags.TAG_MODULE, tags.MODULE_GLOBAL)
        Tags.of(layer).add(tags.TAG_ENVIRONMENT, tags.CURRENT_ENVIRONMENT)

        return layer

    def __create_opensearch_domain(self):
        domain = opensearch.Domain(self, 'OpensearchDomain',
                                   version=opensearch.EngineVersion.OPENSEARCH_1_2,
                                   removal_policy=RemovalPolicy.DESTROY,
                                   enforce_https=True,
                                   enable_version_upgrade=True,
                                   node_to_node_encryption=True,
                                   domain_name='language-analysis',
                                   capacity=opensearch.CapacityConfig(data_nodes=3, master_nodes=3),
                                   ebs=opensearch.EbsOptions(volume_size=50),
                                   encryption_at_rest=opensearch.EncryptionAtRestOptions(enabled=True),
                                   zone_awareness=opensearch.ZoneAwarenessConfig(availability_zone_count=3,
                                                                                 enabled=True))

        Tags.of(domain).add(tags.TAG_MODULE, tags.MODULE_GLOBAL)
        Tags.of(domain).add(tags.TAG_ENVIRONMENT, tags.CURRENT_ENVIRONMENT)

        domain_endpoint_ssm = ssm. \
            StringParameter(self, 'OpensearchDomainSSM',
                            parameter_name=constants.CONFIG_PARAM_OPENSEARCH_DOMAIN_ENDPOINT,
                            string_value=domain.domain_endpoint)

        Tags.of(domain_endpoint_ssm).add(tags.TAG_ENVIRONMENT, tags.CURRENT_ENVIRONMENT)
        Tags.of(domain_endpoint_ssm).add(tags.TAG_MODULE, tags.MODULE_GLOBAL)

        return domain

    def __create_config_files_bucket(self):
        bucket = s3.Bucket(self, 'ConfigFilesBucket',
                           auto_delete_objects=True,
                           removal_policy=RemovalPolicy.DESTROY,
                           bucket_name='config-files-' + self.node.scope.stack_id_termination)

        Tags.of(bucket).add(tags.TAG_ENVIRONMENT, tags.CURRENT_ENVIRONMENT)
        Tags.of(bucket).add(tags.TAG_MODULE, tags.MODULE_GLOBAL)

        s3_deployment.BucketDeployment(self, 'DeployConfigFiles',
                                       sources=[s3_deployment.Source.asset('assets/system_config_files')],
                                       destination_bucket=bucket)

        config_files_bucket_ssm = ssm. \
            StringParameter(self, 'ConfigFilesBucketSSM',
                            parameter_name=constants.CONFIG_PARAM_CONFIG_FILES_BUCKET,
                            string_value=bucket.bucket_name)

        Tags.of(config_files_bucket_ssm).add(tags.TAG_ENVIRONMENT, tags.CURRENT_ENVIRONMENT)
        Tags.of(config_files_bucket_ssm).add(tags.TAG_MODULE, tags.MODULE_GLOBAL)

        return bucket

    def __create_vpc(self):
        vpc = ec2.Vpc(self, 'Vpc', vpc_name='language-analysis-VPC')
        Tags.of(vpc).add(tags.TAG_ENVIRONMENT, tags.CURRENT_ENVIRONMENT)
        Tags.of(vpc).add(tags.TAG_MODULE, tags.MODULE_GLOBAL)

        return vpc

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.layer = self.__create_lambda_layer()
        self.vpc = self.__create_vpc()
        self.opensearch_domain = self.__create_opensearch_domain()
        self.config_files_bucket = self.__create_config_files_bucket()
