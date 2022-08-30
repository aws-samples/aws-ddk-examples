
2
3
4
5
6
7
8
9
10
11
12
13
14
15
16
17
18
19
20
21
22
23
24
25
26
27
28
29
30
31
32
33
34
35
#!/usr/bin/env python3

import aws_cdk as cdk
from aws_ddk_core.cicd import CICDPipelineStack
from aws_ddk_core.config import Config
from ddk_app.ddk_app_stack import DdkApplicationStack

app = cdk.App()

class ApplicationStage(cdk.Stage):
    def __init__(
        self,
        scope,
        environment_id: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, f"Ddk{environment_id.title()}Application", **kwargs)
        DdkApplicationStack(self, "DataPipeline", environment_id)

config = Config()
(
    CICDPipelineStack(
        app,
        id="DdkCodePipeline",
        environment_id="dev",
        pipeline_name="ddk-application-pipeline",
    )
    .add_source_action(repository_name="ddk-repository")
    .add_synth_action()
    .build()
    .add_stage("dev", ApplicationStage(app, "dev", env=config.get_env("dev")))
    .synth()
)

app.synth()