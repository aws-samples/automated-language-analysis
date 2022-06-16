import typing

from aws_cdk import (
    aws_ecr as ecr,
    aws_codecommit as codecommit,
    aws_codebuild as codebuild,
    aws_codepipeline as codepipeline,
    aws_codepipeline_actions as actions,
    aws_s3 as s3,
    RemovalPolicy,
    aws_iam as iam
)

from constructs import Construct


class CodeCommitToECRPipelineProps:
    def __init__(self, ecr_repository_props: ecr.RepositoryProps = None,
                 codecommit_repository_props: codecommit.RepositoryProps = None,
                 codebuild_project_props: codebuild.PipelineProjectProps = None):
        if ecr_repository_props is None:
            ecr_repository_props = ecr.RepositoryProps()

        if codecommit_repository_props is None:
            codecommit_repository_props = codecommit.RepositoryProps()

        if codebuild_project_props is None:
            codebuild_project_props = codebuild.PipelineProjectProps()

        self.ecr_props = ecr_repository_props._values
        self.codecommit_props = codecommit_repository_props._values
        self.codebuild_props = codebuild_project_props._values


class CodeCommitToECRPipeline(Construct):
    @staticmethod
    def buildspec():
        return {
            'version': '0.2',
            'phases': {
                'pre_build': {
                    'commands': [
                        'aws ecr get-login-password --region $AWS_DEFAULT_REGION | docker login --username AWS --password-stdin $AWS_ACCOUNT_ID.dkr.ecr.$AWS_DEFAULT_REGION.amazonaws.com'
                    ]
                },
                'build': {
                    'commands': [
                        'docker build -t $ECR_REPO_NAME:$IMAGE_TAG .',
                        'docker tag $ECR_REPO_NAME:$IMAGE_TAG $AWS_ACCOUNT_ID.dkr.ecr.$AWS_DEFAULT_REGION.amazonaws.com/$ECR_REPO_NAME:$IMAGE_TAG'
                    ]
                },
                'post_build': {
                    'commands': [
                        'docker push $AWS_ACCOUNT_ID.dkr.ecr.$AWS_DEFAULT_REGION.amazonaws.com/$ECR_REPO_NAME:$IMAGE_TAG'
                    ]
                }
            }}

    def __create_ecr_repository(self, **kwargs) -> ecr.Repository:
        return ecr.Repository(self, 'ECRRepository', **kwargs)

    def __create_codecommit_repository(self, **kwargs) -> codecommit.Repository:
        return codecommit.Repository(self, 'CodeCommitRepository', **kwargs)

    def __create_codebuild_project(self, ecr_repository: ecr.Repository, **kwargs) -> codebuild.PipelineProject:
        if 'environment_variables' not in kwargs:
            kwargs['environment_variables'] = {}

        # Set environment variables needed to perform the build
        kwargs['environment_variables'].update(
            {'ECR_REPO_NAME': codebuild.BuildEnvironmentVariable(value=ecr_repository.repository_name),
             'AWS_ACCOUNT_ID': codebuild.BuildEnvironmentVariable(value=self.node.scope.account),
             'IMAGE_TAG': codebuild.BuildEnvironmentVariable(value='latest')
             })

        # Use the default buildspec if none has been specified
        if 'build_spec' not in kwargs:
            kwargs['build_spec'] = codebuild.BuildSpec.from_object_to_yaml(self.buildspec())
        # If user provided a dict as buildspec, covert it to a BuildSpec object
        elif isinstance(kwargs['build_spec'], dict):
            kwargs['build_spec'] = codebuild.BuildSpec.from_object_to_yaml(kwargs['build_spec'])

        kwargs['environment'] = codebuild.BuildEnvironment(privileged=True)

        project = codebuild.PipelineProject(self, "CodebuildProject", **kwargs)
        project.role.add_managed_policy(
            iam.ManagedPolicy.from_managed_policy_arn(self, 'AmazonEC2ContainerRegistryFullAccess',
                                                      'arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryFullAccess')
        )

        return project

    def __create_pipeline(self, codecommit_repository, codebuild_project):
        artifact = codepipeline.Artifact()
        artifact_bucket = s3.Bucket(self, 'ArtifactBucket',
                                    removal_policy=RemovalPolicy.DESTROY,
                                    auto_delete_objects=True)

        pipeline = codepipeline.Pipeline(self, "Codepipeline",
                                         artifact_bucket=artifact_bucket,
                                         pipeline_name='pipeline-{}'.format(codecommit_repository.repository_name),
                                         stages=[
                                             codepipeline.StageProps(
                                                 stage_name='Source-change-detected',
                                                 actions=[
                                                     actions.CodeCommitSourceAction(
                                                         action_name='On-commit-to-main',
                                                         repository=codecommit_repository,
                                                         branch='main',
                                                         output=artifact,
                                                         run_order=1)
                                                 ]),
                                             codepipeline.StageProps(
                                                 stage_name='Build',
                                                 actions=[
                                                     actions.CodeBuildAction(
                                                         action_name='Build-docker-image',
                                                         project=codebuild_project,
                                                         input=artifact,
                                                         run_order=2)
                                                 ])
                                         ])

        return pipeline

    def __init__(self, scope: Construct, id: str, props: typing.Optional[CodeCommitToECRPipelineProps] = None):
        super().__init__(scope, id)

        if props is None:
            props = CodeCommitToECRPipelineProps()

        self.ecr_repository: ecr.Repository = self.__create_ecr_repository(**props.ecr_props)

        self.codecommit_repository: codecommit.Repository = \
            self.__create_codecommit_repository(**props.codecommit_props)

        self.codebuild_project: codebuild.PipelineProject = \
            self.__create_codebuild_project(self.ecr_repository, **props.codebuild_props)

        self.pipeline: codepipeline.Pipeline = \
            self.__create_pipeline(self.codecommit_repository, self.codebuild_project)
