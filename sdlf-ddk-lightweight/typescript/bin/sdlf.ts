#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import { SDLFLightweight } from '../src/sdlf';
import { BaseStack, CICDPipelineStack} from "aws-ddk-core";
import { Construct } from 'constructs';

import { SDLFBaseStack } from "../src/datalake/pipelines";

const app = new cdk.App();
new SDLFLightweight(app, 'SDLFDDKLightweight');



// CI/CD Deployment
class DataLakeFrameworkCICD extends cdk.Stage {
  readonly environmentId: string;
  readonly resourcePrefix: string;
  readonly sdlfParameters: object;
  constructor(scope: Construct, pipelineParams: any, environmentId: string, props: cdk.StageProps) {
    super(scope, `sdlf-ddk-${environmentId}`, props);
      this.environmentId = environmentId;
      this.resourcePrefix = pipelineParams["resource_prefix"] 
        ? pipelineParams["resource_prefix"] 
        : "ddk";
      this.sdlfParameters = pipelineParams["data_pipeline_parameters"] 
        ? pipelineParams["data_pipeline_parameters"] 
        : {};

      new SDLFBaseStack(
        this,
        `${this.resourcePrefix}-data-lake-pipeline`,
        {
          environmentId: this.environmentId,
          resourcePrefix: this.resourcePrefix,
          params: this.sdlfParameters,
        }
      )
  }
}

export class DataLakeFramework extends BaseStack {  // For NO CICD deployments

  constructor(scope: Construct, id: string, environmentId: string, pipelineParams: any) {
    super(scope, id, {});
    
    const resourcePrefix = pipelineParams["resource_prefix"] ?? "ddk";
    new SDLFBaseStack(
      this,
      `${resourcePrefix}-data-lake-pipeline`,
      {
        environmentId: environmentId,
        resourcePrefix: resourcePrefix,
        params: pipelineParams["data_pipeline_parameters"] ?? {}
      }
    )


  }
}



        


satellite_app = cdk.App()
config = Config()
PIPELINE_NAME = "sdlf-ddk-pipeline"
cicd_repository_name = config.get_env_config("cicd").get(
    "repository", "sdlf-ddk-example"
)

cicd_enabled = config.get_env_config("cicd").get("cicd_enabled", False)

if cicd_enabled:
    pipeline = CICDPipelineStack(
        satellite_app,
        id=PIPELINE_NAME,
        environment_id="cicd",
        pipeline_name=PIPELINE_NAME,
        pipeline_args={"publish_assets_in_parallel": True},
    )
    pipeline.add_source_action(repository_name=cicd_repository_name)
    pipeline.add_synth_action()
    pipeline.build()  # type:ignore
    pipeline.add_checks()
    pipeline.add_stage(
        "dev",
        DataLakeFrameworkCICD(
            satellite_app,
            environment_id="dev",
            pipeline_params=config.get_env_config("dev"),
            env=config.get_env("dev"),
        ),
    )
    pipeline.synth()
else:
    DataLakeFramework(
        satellite_app,
        id=f"sdlf-ddk-dev",
        environment_id="dev",
        pipeline_params=config.get_env_config("dev"),
        env=config.get_env("dev"),
    )


satellite_app.synth()
