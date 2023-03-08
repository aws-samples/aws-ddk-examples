import * as cdk from 'aws-cdk-lib';
import * as cr from 'aws-cdk-lib/custom-resources';
import { BaseStack } from 'aws-ddk-core';
import { Construct } from 'constructs';

export interface CustomDatasetConfig {
  team: string;
  dataset: string;
  pipeline: string;
  stageATransform: string;
  registerProvider: cr.Provider;
}

export interface CustomDatasetStackProps extends cdk.StackProps {
  resourcePrefix: string;
  datasetConfig: CustomDatasetConfig;
}

export class CustomDatasetStack extends BaseStack {
  datasetConfig: CustomDatasetConfig;
  team: string;
  pipeline: string;
  dataset: string;
  resourcePrefix: string;
  stageATransform: string;
  constructor(scope: Construct, id: string, props: CustomDatasetStackProps) {
    super(scope, id, props);
    this.datasetConfig = props.datasetConfig;
    this.team = this.datasetConfig.team;
    this.pipeline = this.datasetConfig.pipeline;
    this.dataset = this.datasetConfig.dataset;
    this.resourcePrefix = props.resourcePrefix;
    this.stageATransform = this.datasetConfig.stageATransform;

    this.registerOctagonConfigs(
      this.team,
      this.pipeline,
      this.dataset,
      this.stageATransform
    );
  }
  protected registerOctagonConfigs(
    team: string,
    pipeline: string,
    datasetName: string,
    stageATransform: string
  ): void {
    this.stageATransform = stageATransform ?? 'light_transform_blueprint';

    const props = {
      id: `${team}-${datasetName}`,
      description: `${datasetName} dataset`,
      name: `${team}-${datasetName}`,
      type: 'octagon_dataset',
      pipeline: pipeline,
      version: 1,
      transforms: {
        stageatransform: this.stageATransform
      }
    };

    new cdk.CustomResource(
      this,
      `${props['id']}-${props['type']}-custom-resource`,
      {
        serviceToken: this.datasetConfig.registerProvider.serviceToken,
        properties: { RegisterProperties: props }
      }
    );
  }
}
