import * as path from "path";
import * as cdk from "aws-cdk-lib";
import * as dynamo from "aws-cdk-lib/aws-dynamodb";
import * as kms from "aws-cdk-lib/aws-kms";
import * as lakeformation from "aws-cdk-lib/aws-lakeformation";
import * as iam from "aws-cdk-lib/aws-iam";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as ssm from "aws-cdk-lib/aws-ssm";
import * as s3Deployment from "aws-cdk-lib/aws-s3-deployment";
import * as cr from "aws-cdk-lib/custom-resources";
import { BaseStack, LambdaDefaults, S3Defaults } from "aws-ddk-core";

import { Construct } from "constructs";

export interface FoundationsStackProps extends cdk.StackProps {
  readonly environmentId: string;
  readonly resourcePrefix: string;
  readonly app: string;
  readonly org: string;
  readonly runtime: lambda.Runtime;
}
interface createOctagonTableProps {
  readonly name: string;
  readonly partitionKey: any;
}
export class FoundationsStack extends BaseStack {
  readonly resourcePrefix: string;
  readonly environmentId: string;
  readonly app: string;
  readonly org: string;
  readonly objectMetadata: dynamo.Table;
  readonly datasets: dynamo.Table;
  readonly pipelines: dynamo.Table;
  readonly peh: dynamo.Table;
  readonly registerProvider: cr.Provider;
  readonly lakeformationBucketRegistrationRole: iam.Role;
  readonly rawBucket: s3.Bucket;
  readonly rawBucketKey: kms.Key;
  readonly stageBucket: s3.Bucket;
  readonly stageBucketKey: kms.Key;
  readonly analyticsBucket: s3.Bucket;
  readonly analyticsBucketKey: kms.Key;
  readonly artifactsBucket: s3.Bucket;
  readonly artifactsBucketKey: kms.Key;
  readonly athenaBucket: s3.Bucket;
  readonly athenaBucketKey: kms.Key;
  readonly glueRole: iam.IRole;

  constructor(scope: Construct, id: string, props: FoundationsStackProps) {
    super(scope, `${props.resourcePrefix}-FoundationsStack-${props.environmentId}`, props);
    this.resourcePrefix = props.resourcePrefix
    this.environmentId = props.environmentId
    this.app = props.app;
    this.org = props.org;
    this.objectMetadata = this.createOctagonTable({
      name: `octagon-ObjectMetadata-${props.environmentId}`,
      partitionKey: {name: "id", type: dynamo.AttributeType.STRING},
    })

    this.datasets = this.createOctagonTable({
      name: `octagon-Datasets-${props.environmentId}`,
      partitionKey: {"partition_key": {name: "name", type: dynamo.AttributeType.STRING}},
    })
    this.pipelines = this.createOctagonTable({
      name: `octagon-Pipelines-${props.environmentId}`,
      partitionKey: {"partition_key": {name: "name", type: dynamo.AttributeType.STRING}},
    })
    this.peh = this.createOctagonTable({
      name: `octagon-PipelineExecutionHistory-${props.environmentId}`,
      partitionKey: {"partition_key": {name: "id", type: dynamo.AttributeType.STRING}},
    })
    this.createRegister(props.runtime)

    // creates encrypted buckets and registers them in lake formation
    this.lakeformationBucketRegistrationRole = this.createLakeformationBucketRegistrationRole();
    const [rawBucket, rawBucketKey] = this.createBucket("raw")
    this.rawBucket = rawBucket;
    this.rawBucketKey = rawBucketKey;
    const [stageBucket, stageBucketKey] = this.createBucket("stage")
    this.stageBucket = stageBucket;
    this.stageBucketKey = stageBucketKey;
    const [analyticsBucket, analyticsBucketKey] = this.createBucket("analytics")
    this.analyticsBucket = analyticsBucket;
    this.analyticsBucketKey = analyticsBucketKey;
    const [artifactsBucket, artifactsBucketKey] = this.createBucket("artifacts")
    this.artifactsBucket = artifactsBucket;
    this.artifactsBucketKey = artifactsBucketKey;
    const [athenaBucket, athenaBucketKey] = this.createBucket("athena")
    this.athenaBucket = athenaBucket;
    this.athenaBucketKey = athenaBucketKey;

    // pushes scripts from data_lake/src/glue/ to S3 "artifacts" bucket.
    this.glueRole = this.createSdlfGlueArtifacts()


  }
  private createOctagonTable(props: createOctagonTableProps): dynamo.Table {

    const tableName = props.name.split("-")[1]

    const tableKey = new kms.Key(
      this,
      `${props.name}-table-key`,
      {
        description: `${this.resourcePrefix} ${props.name} Table Key`,
        alias: `${this.resourcePrefix} ${props.name} Table Key`,
        enableKeyRotation: true,
        pendingWindow: cdk.Duration.days(30),
        removalPolicy: cdk.RemovalPolicy.DESTROY
      }
    )
    
    new ssm.StringParameter(
      this,
      `${props.name}-table-name-ssm`,
      {
        parameterName: `/SDLF/DynamoDB/${tableName}`,
        stringValue: props.name,
      }
    )
    return new dynamo.Table(
      this,
      `${props.name}-table`,
      {
        partitionKey: props.partitionKey,
        tableName: props.name,
        encryption: dynamo.TableEncryption.CUSTOMER_MANAGED,
        encryptionKey: tableKey,
        billingMode: dynamo.BillingMode.PAY_PER_REQUEST,
        removalPolicy: cdk.RemovalPolicy.DESTROY,
        pointInTimeRecovery: true,
      }
    )
  }
  private createRegister(runtime: lambda.Runtime): void {
    const registerFunction = new lambda.Function(
      this,
      "register-function",
      LambdaDefaults.functionProps(
        {
          code: lambda.Code.fromAsset(path.join(__dirname, "src/lambdas/register/")),
          handler: "handler.on_event",
          memorySize: 256,
          description: "Registers Datasets, Pipelines and Stages into their respective DynamoDB tables",
          timeout: cdk.Duration.minutes(15),
          runtime: runtime,
          environment: {
             "OCTAGON_DATASET_TABLE_NAME": this.datasets.tableName,
             "OCTAGON_PIPELINE_TABLE_NAME": this.pipelines.tableName
          }
        }
      ),  
    )
    this.datasets.grantReadWriteData(registerFunction)
    this.pipelines.grantReadWriteData(registerFunction)
    const registerProvider = new cr.Provider(
      this,
      "register-provider",
      {
        onEventHandler: registerFunction
      }
    )


  }
  private createBucket(name: string): [s3.Bucket, kms.Key] {
    const bucketKey = new kms.Key(
      this,
      `${this.resourcePrefix}-$${name}-bucket-key`,
      {
        description: `${this.resourcePrefix} $${name} Bucket Key`,
        alias: `${this.resourcePrefix}-$${name}-bucket-key`,
        enableKeyRotation: true,
        pendingWindow: cdk.Duration.days(30),
        removalPolicy: cdk.RemovalPolicy.DESTROY
      }
    )
    new ssm.StringParameter(
      this,
      `${this.resourcePrefix}-$${name}-bucket-key-arn-ssm`,
      {
        parameterName: `/SDLF/KMS/$${name}BucketKeyArn`,
        stringValue: bucketKey.keyArn,
      }
    )

    const bucket = new s3.Bucket(
        this,
        `${this.resourcePrefix}-$${name}-bucket`,
        S3Defaults.bucketProps(
          {
            bucketName: `${this.resourcePrefix}-${this.environmentId}-${cdk.Aws.REGION}-${cdk.Aws.ACCOUNT_ID}-$${name}`,
            encryption: s3.BucketEncryption.KMS,
            encryptionKey: bucketKey,
            accessControl: s3.BucketAccessControl.BUCKET_OWNER_FULL_CONTROL,
            blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
            removalPolicy: cdk.RemovalPolicy.RETAIN,
            eventBridgeEnabled: true
          }
        )
    )

    new ssm.StringParameter(
      this,
      `${this.resourcePrefix}-${name}-bucket-name-ssm`,
      {
        parameterName: `/SDLF/S3/${name}Bucket`,
        stringValue: `${this.resourcePrefix}-${this.environmentId}-${cdk.Aws.REGION}-${cdk.Aws.ACCOUNT_ID}-${name}`,
      }
    )

    new lakeformation.CfnResource(
      this,
      `${this.resourcePrefix}-${name}-bucket-lakeformation-registration`,
      {
        resourceArn: bucket.bucketArn,
        useServiceLinkedRole: false,
        roleArn: this.lakeformationBucketRegistrationRole.roleArn,
      }
    )

    bucketKey.addToResourcePolicy(
      new iam.PolicyStatement(
        {
          effect: iam.Effect.ALLOW,
          actions: [
            "kms:CreateGrant",
            "kms:Decrypt",
            "kms:DescribeKey",
            "kms:Encrypt",
            "kms:GenerateDataKey*",
            "kms:ReEncrypt*",
          ],
          resources: ["*"],
          principals: [this.lakeformationBucketRegistrationRole],
        }
      )
    )
    return [bucket, bucketKey]
  }
  private createLakeformationBucketRegistrationRole(): iam.Role {
    return new iam.Role(
      this,
      "lakeformation-bucket-registration-role",
      {
        assumedBy: new iam.ServicePrincipal("lakeformation.amazonaws.com"),
        inlinePolicies: {
          "LakeFormationDataAccessPolicyForS3": new iam.PolicyDocument(
            {
              statements: [
                new iam.PolicyStatement({
                  effect: iam.Effect.ALLOW,
                  actions: [
                      "s3:GetObject",
                      "s3:GetObjectAttributes",
                      "s3:GetObjectTagging",
                      "s3:GetObjectVersion",
                      "s3:GetObjectVersionAttributes",
                      "s3:GetObjectVersionTagging",
                      "s3:PutObjectTagging",
                      "s3:PutObjectVersionTagging",
                      "s3:PutObject"
                  ],
                  resources:[
                    `arn:aws:s3:::${this.resourcePrefix}-${this.environmentId}-${cdk.Aws.REGION}-${cdk.Aws.ACCOUNT_ID}-*`
                  ]
                }),
                new iam.PolicyStatement({
                  effect: iam.Effect.ALLOW,
                  actions: ["s3:ListBucket"],
                  resources: [
                    `arn:aws:s3:::${this.resourcePrefix}-${this.environmentId}-${cdk.Aws.REGION}-${cdk.Aws.ACCOUNT_ID}-*`
                  ],
                }),
                new iam.PolicyStatement({
                  effect: iam.Effect.ALLOW,
                  actions: ["s3:ListAllMyBuckets"],
                  resources: ["arn:aws:s3:::*"],
                })
              ]
            }
          )
        }
      }
    )
  }
  protected createSdlfGlueArtifacts(): iam.IRole {
    const bucketDeploymentRole = new iam.Role( 
      this,
      `${this.resourcePrefix}-glue-script-s3-deployment-role`,
      {
        assumedBy: new iam.ServicePrincipal("lambda.amazonaws.com"),
        managedPolicies: [
          iam.ManagedPolicy.fromAwsManagedPolicyName("service-role/AWSLambdaBasicExecutionRole")
        ],
      }
    )
    this.artifactsBucketKey.grantEncryptDecrypt(bucketDeploymentRole)
    const gluePath = "data_lake/src/glue/";

    new s3Deployment.BucketDeployment(
      this,
      `${this.resourcePrefix}-glue-script-s3-deployment`,
      {
        sources: [s3Deployment.Source.asset(gluePath)],
        destinationBucket: this.artifactsBucket,
        destinationKeyPrefix: gluePath,
        serverSideEncryptionAwsKmsKeyId: this.artifactsBucketKey.keyId,
        serverSideEncryption: s3Deployment.ServerSideEncryption.AWS_KMS,
        role: bucketDeploymentRole,
      }
    )

    const glueRole= new iam.Role(
      this,
      `${this.resourcePrefix}-glue-stageb-job-role`,
      {
        assumedBy: new iam.ServicePrincipal("glue.amazonaws.com"),
        managedPolicies: [iam.ManagedPolicy.fromAwsManagedPolicyName("service-role/AWSGlueServiceRole")],
      }
    )
    
    new iam.ManagedPolicy(
      this,
      `${this.resourcePrefix}-glue-job-policy`,
      {
        roles: [glueRole],
        document: new iam.PolicyDocument({
          statements: [
            new iam.PolicyStatement(
              {
                effect: iam.Effect.ALLOW,
                actions: [
                  "kms:CreateGrant",
                  "kms:Decrypt",
                  "kms:DescribeKey",
                  "kms:Encrypt",
                  "kms:GenerateDataKey",
                  "kms:GenerateDataKeyPair",
                  "kms:GenerateDataKeyPairWithoutPlaintext",
                  "kms:GenerateDataKeyWithoutPlaintext",
                  "kms:ReEncryptTo",
                  "kms:ReEncryptFrom"
              ],
              resources: [`arn:aws:kms:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:key/*`],
              conditions: {"ForAnyValue:StringLike": {"kms:ResourceAliases": `alias/${this.resourcePrefix}-*-key`}},
              }
            ),
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: ["s3:ListBucket"],
              resources: [
                `arn:aws:s3:::${this.resourcePrefix}-${this.environmentId}-${cdk.Aws.REGION}-${cdk.Aws.ACCOUNT_ID}-*`
              ],
            }),
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: [
                  "s3:GetObject",
                  "s3:PutObject",
                  "s3:DeleteObject",
              ],
              resources: [
                  `arn:aws:s3:::${this.resourcePrefix}-${this.environmentId}-${cdk.Aws.REGION}-${cdk.Aws.ACCOUNT_ID}-*`
              ],
            }),
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: [
                  "lakeformation:DeregisterResource",
                  "lakeformation:GetDataAccess",
                  "lakeformation:GrantPermissions",
                  "lakeformation:PutDataLakeSettings",
                  "lakeformation:GetDataLakeSettings",
                  "lakeformation:RegisterResource",
                  "lakeformation:RevokePermissions",
                  "lakeformation:UpdateResource",
                  "glue:CreateDatabase",
                  "glue:CreateJob",
                  "glue:CreateSecurityConfiguration",
                  "glue:DeleteDatabase",
                  "glue:DeleteJob",
                  "glue:DeleteSecurityConfiguration",
                  "glue:GetDatabase",
                  "glue:GetDatabases",
                  "glue:GetMapping",
                  "glue:GetPartition",
                  "glue:GetPartitions",
                  "glue:GetPartitionIndexes",
                  "glue:GetSchema",
                  "glue:GetSchemaByDefinition",
                  "glue:GetSchemaVersion",
                  "glue:GetSchemaVersionsDif",
                  "glue:GetTable",
                  "glue:GetTables",
                  "glue:GetTableVersion",
                  "glue:GetTableVersions",
                  "glue:GetTags",
                  "glue:PutDataCatalogEncryptionSettings",
                  "glue:SearchTables",
                  "glue:TagResource",
                  "glue:UntagResource",
                  "glue:UpdateDatabase",
                  "glue:UpdateJob",
                  "glue:ListSchemas",
                  "glue:ListSchemaVersions"
              ],
              resources: ["*"],
            }),
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: ["dynamodb:GetItem"],
              resources: [
                `arn:aws:dynamodb:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:table/`,
                `${this.resourcePrefix}-${this.environmentId}-*`,
                `arn:aws:dynamodb:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:table/octagon-*`,
              ],
            }),
          ]
        })
      }
    )
    return glueRole
  }

}
