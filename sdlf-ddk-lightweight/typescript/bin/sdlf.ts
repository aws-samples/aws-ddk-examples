#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import { SDLFLightweight } from '../src/sdlf';
import { BaseStack, CICDPipelineStack, Configurator} from "aws-ddk-core";
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


const satelliteApp = new cdk.App()
const config = new Configurator(app, "./ddk.json")
const pipelineName = "sdlf-ddk-pipeline"
const cicdRepositoryName = config.getEnvConfig("cicd").repository ?? "sdlf-ddk-example";
const cicdEnabled = config.getEnvConfig("cicd")["cicd_enabled"] ?? false;

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
          config.getEnvConfig("dev"),
          "dev",
          {},
      ),
    })
    pipeline.synth()
}
else {
  new DataLakeFrameworkCICD(
    satelliteApp,
    config.getEnvConfig("dev"),
    "dev",
    {},
  )
}
