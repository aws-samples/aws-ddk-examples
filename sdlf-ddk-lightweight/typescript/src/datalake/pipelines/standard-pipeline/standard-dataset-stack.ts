import * as cdk from 'aws-cdk-lib';
import * as events from 'aws-cdk-lib/aws-events';
import * as eventsTargets from 'aws-cdk-lib/aws-events-targets';
import * as glue from 'aws-cdk-lib/aws-glue';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as kms from 'aws-cdk-lib/aws-kms';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as lakeformation from 'aws-cdk-lib/aws-lakeformation';
import * as cr from 'aws-cdk-lib/custom-resources';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import * as ssm from 'aws-cdk-lib/aws-ssm';
import { BaseStack, BaseStackProps, KmsDefaults } from 'aws-ddk-core';
import { Construct } from 'constructs';

export interface StandardDatasetConfig {
  team: string;
  dataset: string;
  pipeline: string;
  app: string;
  org: string;
  routingB: lambda.IFunction;
  stageATransform: string;
  stageBTransform: string;
  artifactsBucket: s3.IBucket;
  artifactsBucketKey: kms.IKey;
  stageBucket: s3.IBucket;
  stageBucketKey: kms.IKey;
  glueRole: iam.IRole;
  registerProvider: cr.Provider;
}

export interface StandardDatasetStackProps extends BaseStackProps {
  environmentId: string;
  resourcePrefix: string;
  config: StandardDatasetConfig;
}

export class StandardDatasetStack extends BaseStack {
  readonly datasetConfig: StandardDatasetConfig;
  readonly resourcePrefix: string;
  readonly team: string;
  readonly pipeline: string;
  readonly dataset: string;
  readonly org: string;
  readonly app: string;
  readonly routingB: lambda.IFunction;
  readonly environmentId: string;
  stageATransform: string;
  stageBTransform: string;
  readonly database: glue.CfnDatabase;

  constructor(scope: Construct, id: string, props: StandardDatasetStackProps) {
    super(scope, id, props);
    this.datasetConfig = props.config;
    this.resourcePrefix = props.resourcePrefix;
    this.environmentId = props.environmentId;
    this.team = this.datasetConfig.team;
    this.pipeline = this.datasetConfig.pipeline;
    this.dataset = this.datasetConfig.dataset;
    this.org = this.datasetConfig.org;
    this.app = this.datasetConfig.app;
    this.routingB = this.datasetConfig.routingB;
    this.stageATransform = this.datasetConfig.stageATransform;
    this.stageBTransform = this.datasetConfig.stageBTransform;

    const gluePath = `datalake/src/glue/pyshell_scripts/sdlf_heavy_transform/${this.team}/${this.dataset}/main.py`;
    this.createStageBGlueJob(this.team, this.dataset, gluePath);
    this.registerOctagonConfigs(
      this.team,
      this.pipeline,
      this.dataset,
      this.stageATransform,
      this.stageBTransform
    );
    this.database = this.createGlueDatabase(this.team, this.dataset);
    this.createRoutingQueue();

    // Add stage b scheduled rule every 5 minutes
    const scheduledRule = new events.Rule(
      this,
      `${this.resourcePrefix}-${this.team}-${this.pipeline}-${this.dataset}-schedule-rule`,
      {
        schedule: events.Schedule.rate(cdk.Duration.minutes(5))
      }
    );
    scheduledRule.addTarget(
      new eventsTargets.LambdaFunction(this.routingB, {
        event: events.RuleTargetInput.fromObject({
          team: this.team,
          pipeline: this.pipeline,
          pipelinestage: 'StageB',
          dataset: this.dataset,
          org: this.org,
          app: this.app,
          env: this.environmentId,
          databasename: this.database.ref
        })
      })
    );
  }
  protected createStageBGlueJob(
    team: string,
    datasetName: string,
    codePath: string
  ): void {
    new glue.CfnJob(
      this,
      `${this.resourcePrefix}-heavy-transform-${team}-${datasetName}-job`,
      {
        name: `${this.resourcePrefix}-${team}-${datasetName}-glue-job`,
        glueVersion: '2.0',
        allocatedCapacity: 2,
        executionProperty: {
          maxConcurrentRuns: 4
        },
        command: {
          name: 'glueetl',
          scriptLocation: `s3://{this.datasetConfig.artifactsBucket.bucketname}/{codePath}`
        },
        defaultArguments: {
          '--job-bookmark-option': 'job-bookmark-enable',
          '--enable-metrics': '',
          '--additional-python-modules': 'awswrangler==2.4.0',
          '--enable-glue-datacatalog': ''
        },
        role: this.datasetConfig.glueRole.roleArn
      }
    );
  }
  protected registerOctagonConfigs(
    team: string,
    pipeline: string,
    datasetName: string,
    stageATransform?: string,
    stageBTransform?: string
  ): void {
    this.stageATransform = stageATransform ?? 'light_transform_blueprint';
    this.stageBTransform = stageBTransform ?? 'heavy_transform_blueprint';

    const props = {
      id: `${team}-${datasetName}`,
      description: `${datasetName} dataset`,
      name: `${team}-${datasetName}`,
      type: 'octagon_dataset',
      pipeline: pipeline,
      maxitemsprocess: {
        stageb: 100,
        stagec: 100
      },
      minitemsprocess: {
        stageb: 1,
        stagec: 1
      },
      version: 1,
      transforms: {
        stageATransform: this.stageATransform,
        stageBTransform: this.stageBTransform
      }
    };

    const serviceSetupProperties = { RegisterProperties: props };

    new cdk.CustomResource(
      this,
      `${props['id']}-${props['type']}-custom-resource`,
      {
        serviceToken: this.datasetConfig.registerProvider.serviceToken,
        properties: serviceSetupProperties
      }
    );
  }
  protected createGlueDatabase(
    team: string,
    datasetName: string
  ): glue.CfnDatabase {
    new lakeformation.CfnDataLakeSettings(
      this,
      `${this.resourcePrefix}-${team}-${datasetName}-DataLakeSettings`,
      {
        admins: [
          {
            dataLakePrincipalIdentifier: this.datasetConfig.glueRole.roleArn
          }
        ]
      }
    );

    const database = new glue.CfnDatabase(
      this,
      `${this.resourcePrefix}-${team}-${datasetName}-database`,
      {
        databaseInput: {
          name: `awsdatalake{this.environmentId}${team}${datasetName}db`,
          locationUri: `s3://{this.datasetConfig.stagebucket.bucketname}/post-stage/${team}/${datasetName}`
        },
        catalogId: cdk.Aws.ACCOUNT_ID
      }
    );

    new lakeformation.CfnPermissions(
      this,
      `${this.resourcePrefix}-${team}-${datasetName}-glue-job-database-lakeformation-permissions`,
      {
        dataLakePrincipal: {
          dataLakePrincipalIdentifier: this.datasetConfig.glueRole.roleArn
        },
        resource: {
          databaseResource: {
            name: this.database.ref
          }
        },
        permissions: ['CREATETABLE', 'ALTER', 'DROP']
      }
    );
    return database;
  }
  protected createRoutingQueue(): void {
    const sqsKey = new kms.Key(
      this,
      `${this.resourcePrefix}-${this.team}-${this.dataset}-sqs-key-b`,
      {
        description: `${this.resourcePrefix} SQS Key Stage B`,
        alias: `${this.resourcePrefix}-${this.team}-${this.dataset}-sqs-stage-b-key`,
        enableKeyRotation: true,
        pendingWindow: cdk.Duration.days(30),
        removalPolicy: cdk.RemovalPolicy.DESTROY
      }
    );

    new ssm.StringParameter(
      this,
      `${this.resourcePrefix}-${this.team}-${this.dataset}-dlq-b.fifo-ssm`,
      {
        parameterName: `/SDLF/SQS/${this.team}/${this.dataset}StageBDLQ`,
        stringValue: `${this.resourcePrefix}-${this.team}-${this.dataset}-dlq-b.fifo`
      }
    );

    new sqs.Queue(
      this,
      `${this.resourcePrefix}-${this.team}-${this.dataset}-queue-b.fifo`,
      {
        queueName: `${this.resourcePrefix}-${this.team}-${this.dataset}-queue-b.fifo`,
        fifo: true,
        visibilityTimeout: cdk.Duration.seconds(60),
        encryption: sqs.QueueEncryption.KMS,
        encryptionMasterKey: sqsKey,
        deadLetterQueue: {
          maxReceiveCount: 1,
          queue: new sqs.Queue(
            this,
            `${this.resourcePrefix}-${this.team}-${this.dataset}-dlq-b.fifo`,
            {
              queueName: `${this.resourcePrefix}-${this.team}-${this.dataset}-dlq-b.fifo`,
              fifo: true,
              visibilityTimeout: cdk.Duration.seconds(60),
              encryption: sqs.QueueEncryption.KMS,
              encryptionMasterKey: sqsKey
            }
          )
        }
      }
    );

    new ssm.StringParameter(
      this,
      `${this.resourcePrefix}-${this.team}-${this.dataset}-queue-b.fifo-ssm`,
      {
        parameterName: `/SDLF/SQS/${this.team}/${this.dataset}StageBQueue`,
        stringValue: `${this.resourcePrefix}-${this.team}-${this.dataset}-queue-b.fifo`
      }
    );
  }
}
