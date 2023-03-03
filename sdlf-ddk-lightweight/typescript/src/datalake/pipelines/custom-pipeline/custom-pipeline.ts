import * as path from "path";
import * as cdk from "aws-cdk-lib";
import * as iam from "aws-cdk-lib/aws-iam";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as ssm from "aws-cdk-lib/aws-ssm";
import { BaseStack, S3EventStage, DataPipeline, LambdaDefaults, BaseStackProps } from "aws-ddk-core";
import { Construct } from "constructs";
import { FoundationsStack } from "../../foundations";
import { SDLFLightTransform, SDLFLightTransformConfig } from "../common-stages";
import { CustomDatasetConfig, CustomDatasetStack } from "./custom-dataset-stack";

const PIPELINE_TYPE = "custom";

function getSsmValue(scope: Construct, id: string, parameterName: string): string {
    return ssm.StringParameter.fromStringParameterName(
        scope,
        id,
        parameterName,
    ).stringValue
}

export interface CustomPipelineProps extends BaseStackProps {
    readonly environmentId: string,
    readonly resourcePrefix: string;
    readonly team: string;
    readonly foundationsStage: FoundationsStack;
    readonly wranglerLayer: lambda.ILayerVersion;
    readonly app: string;
    readonly org: string;
    readonly runtime: lambda.Runtime;
}
export class CustomPipeline extends BaseStack {
    readonly team: string;
    readonly resourcePrefix: string;
    readonly pipelineId: string;
    readonly environmentId: string;
    readonly wranglerLayer: lambda.ILayerVersion;
    readonly foundationsStage: FoundationsStack;
    readonly app: string;
    readonly org: string;
    readonly runtime: lambda.Runtime;
    readonly datalakeLibraryLayerArn: string;
    readonly datalakeLibraryLayer: lambda.ILayerVersion;
    datalakePipeline: DataPipeline;
    s3EventCaptureStage: S3EventStage;
    datalakeLightTransform: SDLFLightTransform;

    constructor(scope: Construct, id: string, props: CustomPipelineProps) {
        super(scope, id, props);
        this.team = props.team
        this.resourcePrefix = props.resourcePrefix
        this.pipelineId = `${this.resourcePrefix}-${this.team}-${PIPELINE_TYPE}`
        this.wranglerLayer = props.wranglerLayer
        this.foundationsStage = props.foundationsStage
        this.app = props.app
        this.org = props.org
        this.environmentId = props.environmentId
        this.runtime = props.runtime

        this.datalakeLibraryLayerArn = getSsmValue(
            this,
            "data-lake-library-layer-arn-ssm",
            "/SDLF/Layer/DataLakeLibrary",
        )
        this.datalakeLibraryLayer = lambda.LayerVersion.fromLayerVersionArn(
            this,
            "data-lake-library-layer",
            this.datalakeLibraryLayerArn,   
        )
        this.createCustomPipeline()
    }
    protected createCustomPipeline(): void {
        // routing function
        const routingFunction = this.createRoutingLambda()

        // S3 Event Capture Stage
        this.s3EventCaptureStage = new S3EventStage(
            this,
            `${this.pipelineId}-s3-event-capture`,
            {
            eventNames: [
                "Object Created"
            ],
            bucket: this.foundationsStage.rawBucket
            }
        )

        this.datalakeLightTransform = new SDLFLightTransform(
            this,
            `${this.pipelineId}-stage-a`,
            {
                name: `${this.resourcePrefix}-SDLFLightTransform-${this.team}-${PIPELINE_TYPE}-${this.environmentId}`,
                prefix: this.resourcePrefix,
                environmentId: this.environmentId,
                config: {
                    team: this.team,
                    pipeline: PIPELINE_TYPE,
                    rawBucket: this.foundationsStage.rawBucket,
                    rawBucketKey: this.foundationsStage.rawBucketKey,
                    stageBucket: this.foundationsStage.stageBucket,
                    stageBucketKey: this.foundationsStage.stageBucketKey,
                    routingLambda: routingFunction,
                    datalakeLib: this.datalakeLibraryLayer,
                    registerProvider: this.foundationsStage.registerProvider,
                    wranglerLayer: this.wranglerLayer,
                    runtime: lambda.Runtime.PYTHON_3_9
                },
                props: {
                    "version": 1,
                    "status": "ACTIVE",
                    "name": `${this.team}-${PIPELINE_TYPE}-stage-a`,
                    "type": "octagonpipeline",
                    "description": `${this.resourcePrefix} data lake light transform`,
                    "id": `${this.team}-${PIPELINE_TYPE}-stage-a`
                },
                description: `${this.resourcePrefix} data lake light transform`,
            }
        )

        this.datalakePipeline = 
            new DataPipeline(
                this,
                this.pipelineId,
                {
                name: `${this.pipelineId}-pipeline`,
                description: `${this.resourcePrefix} data lake pipeline`,
                }
            )
            .addStage({stage: this.s3EventCaptureStage})
            .addStage({stage: this.datalakeLightTransform, skipRule: true})
    }
    protected createRoutingLambda(): lambda.IFunction {
        const routingFunction = new lambda.Function(
            this,
            `${this.resourcePrefix}-${this.team}-${PIPELINE_TYPE}-pipeline-routing-function`,
            {
                functionName: `${this.resourcePrefix}-${this.team}-${PIPELINE_TYPE}-pipeline-routing`,
                code: lambda.Code.fromAsset(
                    path.join(__dirname, "../../src/lambdas/routing/")
                ),
                handler: "handler.lambdahandler",
                description: "routes to the right team and pipeline",
                timeout: cdk.Duration.seconds(60),
                memorySize: 256,
                runtime: this.runtime,
                environment: {
                    "ENV": this.environmentId,
                    "APP": this.app,
                    "ORG": this.org,
                    "PREFIX": this.resourcePrefix
                },
            }
        )
        this.foundationsStage.objectMetadata.grantReadWriteData(routingFunction)
        this.foundationsStage.datasets.grantReadWriteData(routingFunction)
        routingFunction.addToRolePolicy(
            new iam.PolicyStatement({
                effect:iam.Effect.ALLOW,
                actions:[
                    "kms:CreateGrant",
                    "kms:Decrypt",
                    "kms:DescribeKey",
                    "kms:Encrypt",
                    "kms:GenerateDataKey",
                    "kms:GenerateDataKeyPair",
                    "kms:GenerateDataKeyPairWithoutPlaintext",
                    "kms:GenerateDataKeyWithoutPlaintext",
                    "kms:ReEncryptTo",
                    "kms:ReEncryptFrom",
                    "kms:ListAliases",
                    "kms:ListGrants",
                    "kms:ListKeys",
                    "kms:ListKeyPolicies"
                ],
                resources:["*"],
                conditions: {
                    "ForAnyValue:StringLike": {
                        "kms:ResourceAliases": "alias/*"
                    }
                }
            })
        )
        routingFunction.addToRolePolicy(
            new iam.PolicyStatement({
                effect:iam.Effect.ALLOW,
                actions:[
                    "sqs:SendMessage",
                    "sqs:DeleteMessage",
                    "sqs:ReceiveMessage",
                    "sqs:GetQueueAttributes",
                    "sqs:ListQueues",
                    "sqs:GetQueueUrl",
                    "sqs:ListDeadLetterSourceQueues",
                    "sqs:ListQueueTags"
                ],
                resources:[`$arn:aws:sqs:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:${this.resourcePrefix}-*`],
            })
        )
        routingFunction.addToRolePolicy(
            new iam.PolicyStatement({
                effect:iam.Effect.ALLOW,
                actions:[
                    "ssm:GetParameter",
                    "ssm:GetParameters"
                ],
                resources:[`$arn:aws:ssm:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:parameter/SDLF/*`],
            })
        )

        routingFunction.addPermission(
            "invoke-lambda-eventbridge",
            {
            principal: new iam.ServicePrincipal("events.amazonaws.com"),
            action: "lambda:InvokeFunction"
            }
        )
        return routingFunction
    }
    public registerDataset(dataset: string, config: any): void {
         // Create dataset stack
        const stageATransform = config["stage_a_transform"] ?? "sdlf_light_transform"

        new CustomDatasetStack(
            this,
            `${this.team}-${PIPELINE_TYPE}-${dataset}-dataset-stage`,
            {
                resourcePrefix: this.resourcePrefix,
                config: {
                    team: this.team,
                    dataset: dataset,
                    pipeline: PIPELINE_TYPE,
                    stageATransform: stageATransform,
                    registerProvider: this.foundationsStage.registerProvider
                }
            }
        )

        // Add S3 object created event pattern
        const baseEventPattern = this.s3EventCaptureStage.eventPattern
        if (baseEventPattern && baseEventPattern.detail) {
            baseEventPattern.detail["object"] = {
                "key": [
                    {
                        "prefix": `${this.team}/{dataset}/`
                    }
                ]
            }
        }

        this.datalakePipeline.addRule(
            {
                eventPattern: baseEventPattern,
                eventTargets: this.datalakeLightTransform.targets
            }
        )
    }

}


        
        