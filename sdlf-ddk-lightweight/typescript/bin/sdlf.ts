#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import { BaseStack, CICDPipelineStack, Configurator} from "aws-ddk-core";
import { Construct } from 'constructs';

import { SDLFBaseStack } from "../src/datalake/pipelines";


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


const satelliteApp = new cdk.App()
const cicdConfig = new Configurator(satelliteApp, "./ddk.json", "cicd")
const devConfig = new Configurator(satelliteApp, "./ddk.json", "dev")
const pipelineName = "sdlf-ddk-pipeline"
const cicdRepositoryName = cicdConfig.getEnvConfig("repository") ?? "sdlf-ddk-example";
const cicdEnabled = cicdConfig.getEnvConfig("cicd_enabled") ?? false;

if (cicdEnabled) {
    const pipeline = new CICDPipelineStack(
        satelliteApp,
        pipelineName,
        {
          environmentId: "cicd",
          pipelineName: pipelineName,
        }
    )
    pipeline.addSourceAction({repositoryName: cicdRepositoryName})
    pipeline.addSynthAction()
    pipeline.buildPipeline({publishAssetsInParallel: true})
    pipeline.addChecks()
    pipeline.addStage({
      stageId: "dev",
      stage: new DataLakeFrameworkCICD(
          satelliteApp,
          devConfig.getEnvConfig("dev"),
          "dev",
          {},
      ),
    })
    pipeline.synth()
}
else {
  new DataLakeFrameworkCICD(
    satelliteApp,
    devConfig.config,
    "dev",
    {},
  )
}
