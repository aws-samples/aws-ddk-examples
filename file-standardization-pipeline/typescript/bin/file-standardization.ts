import * as cdk from 'aws-cdk-lib';
import * as glue from 'aws-cdk-lib/aws-glue';
import * as glueAlpha from '@aws-cdk/aws-glue-alpha';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as ddk from 'aws-ddk-core';
import { Construct } from 'constructs';

export class FileStandardizationPipelineStack extends ddk.BaseStack {
  readonly bucket: s3.IBucket;
  constructor(scope: Construct, id: string, props: ddk.BaseStackProps) {
    super(scope, id, props);

    this.bucket = new s3.Bucket(this, 'Bucket', {
      bucketName: `ddk-file-standardization-${this.region}-${this.account}`,
      eventBridgeEnabled: true
    });
    this.bucket.addToResourcePolicy(
      new iam.PolicyStatement({
        sid: 'AllowGlueActions',
        principals: [new iam.ServicePrincipal('glue.amazonaws.com')],
        actions: [
          's3:Put*',
          's3:Get*',
          's3:AbortMultipartUpload',
          's3:ListMultipartUploadParts',
          's3:ListBucketMultipartUploads'
        ],
        resources: [this.bucket.bucketArn, `${this.bucket.bucketArn}/*`],
        conditions: {
          StringEquals: {
            'aws:SourceAccount': this.account
          }
        }
      })
    );

    const s3EventStage = new ddk.S3EventStage(this, 'S3 Event Stage', {
      eventNames: ['Object Created'],
      bucket: this.bucket,
      keyPrefix: 'input_files/'
    });

    const database = new glue.CfnDatabase(this, 'Glue Database', {
      catalogId: this.account,
      databaseInput: {
        description: 'glue database created by the ddk'
      }
    });
    const glueCrawlerRole = new iam.Role(this, 'Glue Crawler Role', {
      assumedBy: new iam.ServicePrincipal('glue.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName(
          'service-role/AWSGlueServiceRole'
        )
      ]
    });
    glueCrawlerRole.addToPolicy(
      new iam.PolicyStatement({
        actions: ['s3:GetObject', 's3:PutObject'],
        resources: [`${this.bucket.bucketArn}/output/*`]
      })
    );

    const glueTransformStage = new ddk.GlueTransformStage(
      this,
      'Glue Transform Stage',
      {
        jobProps: {
          maxConcurrentRuns: 100,
          executable: glueAlpha.JobExecutable.pythonEtl({
            glueVersion: glueAlpha.GlueVersion.V3_0,
            pythonVersion: glueAlpha.PythonVersion.THREE,
            script: glueAlpha.Code.fromAsset(
              './assets/file_standardization/glue_script.py'
            )
          })
        },
        jobRunArgs: {
          '--additional-python-modules': 'pyarrow==3,awswrangler',
          '--input_s3_path.$': '$.input_s3_path',
          '--target_s3_path.$': '$.target_s3_path'
        },
        crawlerProps: {
          databaseName: database.ref,
          targets: {
            s3Targets: [
              {
                path: `s3://${this.bucket.bucketName}/output}`
              }
            ]
          },
          role: glueCrawlerRole.roleArn
        }
      }
    );
    glueTransformStage.stateMachine.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ['glue:StartCrawler'],
        resources: [
          `arn:aws:glue:${this.region}:${this.account}:crawler/${glueTransformStage.crawler?.ref}`
        ]
      })
    );
    this.bucket.grantReadWrite(glueTransformStage.glueJob);

    const sqsToLambdaStage = new ddk.SqsToLambdaStage(
      this,
      'Sqs To Lambda Stage',
      {
        lambdaFunctionProps: {
          code: lambda.Code.fromAsset('./assets/invoke_step_function'),
          handler: 'handler.lambda_handler',
          runtime: lambda.Runtime.PYTHON_3_9,
          environment: {
            STEPFUNCTION: glueTransformStage.stateMachine.stateMachineArn
          }
        },
        batchSize: 1
      }
    );
    sqsToLambdaStage.function.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ['states:StartExecution'], // verify if this is necessary
        resources: [glueTransformStage.stateMachine.stateMachineArn]
      })
    );

    new ddk.DataPipeline(this, 'File Standardization Pipeline', {
      description: 'File Standardization Pipeline Using the AWS DDK'
    })
      .addStage({ stage: s3EventStage })
      .addStage({ stage: sqsToLambdaStage })
      .addStage({ stage: glueTransformStage, skipRule: true });
  }
}

const app = new cdk.App();
new FileStandardizationPipelineStack(app, 'FileStandardizationExample', {});
