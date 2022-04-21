#!/usr/bin/env python3

import aws_cdk as cdk
from aws_ddk_core.cicd import CICDPipelineStack, get_codeartifact_publish_action
from aws_ddk_core.config import Config
from ddk_lib.artifactory_stack import ArtifactoryStack

app = cdk.App()

# Artifactory parameters
ENVIRONMENT_ID = "dev"
DOMAIN_NAME = "ddk-lib-domain"
env_config = Config().get_env_config(environment_id=ENVIRONMENT_ID)
DOMAIN_OWNER = env_config["account"]
REPOSITORY_NAME = "ddk-lib-repository"
PIPELINE_NAME = "ddk-lib-pipeline"

# Private artifactory stack
artifactory_stack: ArtifactoryStack = ArtifactoryStack(
    app,
    id="DdkArtifactory",
    environment_id="dev",
    domain_name=DOMAIN_NAME,
    domain_owner=DOMAIN_OWNER,
    repository_name=REPOSITORY_NAME,
)

# Artifactory CI/CD pipeline
pipeline: CICDPipelineStack = (
    CICDPipelineStack(
        app,
        id="DdkArtifactoryCodePipeline",
        environment_id=ENVIRONMENT_ID,
        pipeline_name=PIPELINE_NAME,
    )
    .add_source_action(repository_name=REPOSITORY_NAME)
    .add_synth_action()
    .build()
    .add_custom_stage(
        stage_name="PublishToCodeArtifact",
        steps=[
            get_codeartifact_publish_action(
                partition="aws",
                region=env_config["region"],
                account=env_config["account"],
                codeartifact_repository=REPOSITORY_NAME,
                codeartifact_domain=DOMAIN_NAME,
                codeartifact_domain_owner=DOMAIN_OWNER,
            )
        ],
    )
)

app.synth()
