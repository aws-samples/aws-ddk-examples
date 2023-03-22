#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import { BaseStack, BaseStackProps, CICDPipelineStack, Configurator, getConfig} from "aws-ddk-core";
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
      this.resourcePrefix = pipelineParams.resource_prefix
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

  constructor(scope: Construct, environmentId: string, props: BaseStackProps, pipelineParams?: any) {
    const resourcePrefix = pipelineParams["resource_prefix"] ?? "ddk";
    const id = `${resourcePrefix}-data-lake-pipeline`;
    super(scope, id, props);
    new SDLFBaseStack(
      this,
      id,
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
const pipelineName = "sdlf-ddk-pipeline"
const cicdRepositoryName = cicdConfig.getConfigAttribute("repository") ?? "sdlf-ddk-example";
const cicdEnabled = cicdConfig.getConfigAttribute("cicd_enabled") ?? false;

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
    pipeline.addStage({
      stageId: "dev",
      stage: new DataLakeFrameworkCICD(
          satelliteApp,
          getConfig({}).environments.dev,
          "dev",
          {},
      ),
    })
    pipeline.synth()
}
else {
  new DataLakeFramework(
    satelliteApp,
    "dev",
    {},
    getConfig({}).environments.dev,
  )
}
