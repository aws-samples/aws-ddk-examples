import * as cdk from "aws-cdk-lib";
import * as events from "aws-cdk-lib/aws-events";
import * as eventTargets from "aws-cdk-lib/aws-events-targets";
import * as kms from "aws-cdk-lib/aws-kms";
import * as iam from "aws-cdk-lib/aws-iam";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as ssm from "aws-cdk-lib/aws-ssm";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as sfn from "aws-cdk-lib/aws-stepfunctions";
import * as tasks from "aws-cdk-lib/aws-stepfunctions-tasks";
import * as cr from "aws-cdk-lib/custom-resources";
import { assignLambdaFunctionProps, assignKmsKeyProps, SqsToLambdaStage, StateMachineStage, StateMachineStageProps } from "aws-ddk-core";
import { Construct } from "constructs";



export interface SDLFLightTransformConfig {
    readonly team: string;
    readonly pipeline: string;
    readonly rawBucket: s3.IBucket;
    readonly rawBucketKey: kms.IKey;
    readonly stagebucket: s3.IBucket;
    readonly stageBucketKey: kms.IKey;
    readonly routingLambda: lambda.IFunction;
    readonly dataLakeLib: lambda.ILayerVersion;
    readonly registerProvider: cr.Provider;
    readonly wranglerLayer: lambda.ILayerVersion;
    readonly runtime: lambda.Runtime;
}

export interface SDLFLightTransformProps extends StateMachineStageProps {
    readonly scope: Construct;
    readonly constructid: string;
    readonly name: string;
    readonly prefix: string;
    readonly environmentId: string;
    readonly config: SDLFLightTransformConfig;
    readonly props: object;
}

export class SDLFLightTransform extends StateMachineStage {
    readonly targets?: events.IRuleTarget[];
    readonly config: SDLFLightTransformConfig;
    readonly environmentId: string;
    readonly prefix: string;
    readonly team: string;
    readonly pipeline: string;
    readonly lambdaRole: iam.Role;
    readonly redriveLambda: lambda.Function;
    readonly routingLambda: lambda.Function;
    readonly props: object;
    readonly routingQueue: sqs.Queue;
    readonly routingDLQ: sqs.Queue;
    readonly sqsKey: kms.Key;

    constructor(scope: Construct, id: string, props: SDLFLightTransformProps) {
        super(scope, id, props);
        this.config = props.config;
        this.environmentId = props.environmentId;
        this.prefix = props.prefix;
        this.props = props.props;
        
        const serviceSetupProperties = {"RegisterProperties": this.props}
        new cdk.CustomResource(
            this,
            `{this.props['id']}-{this.props['type']}-custom-resource`,
            {
                serviceToken: this.config.registerProvider.serviceToken,
                properties: serviceSetupProperties
            }
        )
        this.team = this.config.team
        this.pipeline = this.config.pipeline

        [this.routingQueue, this.routingDLQ, this.sqsKey] = this.createRoutingQueues()

        this.lambdaRole = this.createLambdaRole()
        const routingLambda = this.createLambdaFunction("routing", timeoutminutes=1);
        new SqsToLambdaStage(
            this,
            `${this.prefix}-routing-${this.team}-${this.pipeline}-sqs-lambda`,
            {
                lambdaFunction: routingLambda,
                sqsQueue: this.routingQueue,
                messageGroupId: `${this.prefix}-routing-${this.team}-${this.pipeline}-group`
            }
        )
        this.createLambdaFunction("redrive")
        const preupdateTask = this.createLambdaTask("preupdate")
        const processTask = this.createLambdaTask("process", "$.Payload.body.processedKeys", 1536)
        const postupdateTask = this.createLambdaTask("postupdate", "$.statusCode")
        const errorTask = this.createLambdaTask("error")
        this.buildStateMachine(preupdateTask, processTask, postupdateTask, errorTask)

        this.targets = new eventTargets.LambdaFunction(this.config.routingLambda)
    
    }
    protected createRoutingQueues(): [sqs.IQueue, sqs.IQueue, kms.IKey] {
        const sqsKey = new kms.Key(
            this,
            `${this.prefix}-${this.team}-${this.pipeline}-sqs-key-a`,
            assignKmsKeyProps(
            {
                description:`${this.prefix} SQS Key Stage A`,
                alias:`${this.prefix}-${this.team}-${this.pipeline}-sqs-stage-a-key`,
                enableKeyRotation: true,
                pendingWindow: cdk.Duration.days(30),
                removalPolicy: cdk.RemovalPolicy.DESTROY,
            }
            )
        )

        const routingDeadLetterQueue = new sqs.Queue(
            this,
            `${this.prefix}-${this.team}-${this.pipeline}-dlq-a.fifo`,
            {
                queueName: `${this.prefix}-${this.team}-${this.pipeline}-dlq-a.fifo`,
                fifo: true,
                visibilityTimeout: cdk.Duration.seconds(60),
                encryption: sqs.QueueEncryption.KMS,
                encryptionMasterKey: sqsKey
            }
        )

        new ssm.StringParameter(
            this,
            `${this.prefix}-${this.team}-${this.pipeline}-dlq-a.fifo-ssm`,
            {
                parameterName: `/SDLF/SQS/${this.team}/${this.pipeline}StageADLQ`,
                stringValue: `${this.prefix}-${this.team}-${this.pipeline}-dlq-a.fifo`,
            }
        )

        const routingQueue = new sqs.Queue(
            this,
            `${this.prefix}-${this.team}-${this.pipeline}-queue-a.fifo`,
            {
                queueName: `${this.prefix}-${this.team}-${this.pipeline}-queue-a.fifo`,
                fifo: true,
                contentBasedDeduplication: true,
                visibilityTimeout: cdk.Duration.seconds(60),
                encryption: sqs.QueueEncryption.KMS,
                encryptionMasterKey: sqsKey,
                deadLetterQueue: {
                    maxReceiveCount: 1,
                    queue: routingDeadLetterQueue
                }
            }
        )

        new ssm.StringParameter(
            this,
            `${this.prefix}-${this.team}-${this.pipeline}-queue-a.fifo-ssm`,
            {
            parameterName: `/SDLF/SQS/${this.team}/${this.pipeline}StageAQueue`,
            stringValue: `${this.prefix}-${this.team}-${this.pipeline}-queue-a.fifo`,
            }
        )

        return [routingQueue, routingDeadLetterQueue, sqsKey]
    }
    protected createLambdaRole(): iam.IRole {
        const role = new iam.Role(
            this,
            `${this.prefix}-lambda-role-${this.team}-${this.pipeline}-a`,
            {
                roleName: `${this.prefix}-lambda-role-${this.team}-${this.pipeline}-a`,
                assumedBy: new iam.ServicePrincipal("lambda.amazonaws.com"),
                managedPolicies: [
                    iam.ManagedPolicy.fromAwsManagedPolicyName("service-role/AWSLambdaBasicExecutionRole")
                ],
            }   
        )

        new iam.ManagedPolicy(
            this,
            `${this.prefix}-lambda-policy-${this.team}-${this.pipeline}-b`,
            {
            managedPolicyName: `${this.prefix}-lambda-policy-${this.team}-${this.pipeline}-b`,
            roles: [role],
            document: new iam.PolicyDocument({
                statements: [
                    new iam.PolicyStatement({
                        effect: iam.Effect.ALLOW,
                        actions: [
                            "glue:StartJobRun",
                            "glue:GetJobRun"
                        ],
                        resources: ["*"],
                    }),
                    new iam.PolicyStatement({
                        effect: iam.Effect.ALLOW,
                        actions: [
                            "kms:CreateGrant",
                            "kms:Decrypt",
                            "kms:DescribeKey",
                            "kms:Encrypt",
                            "kms:GenerateDataKey*",
                            "kms:ReEncrypt*",
                            "kms:ListAliases"
                        ],
                        resources: ["*"],
                        conditions: {
                            "ForAnyValue:StringLike": {
                                "kms:ResourceAliases": [
                                    `alias/${this.prefix}-octagon-*`,
                                    `alias/${this.prefix}-${this.team}-*`
                                ]
                            }
                        }
                    }),
                    new iam.PolicyStatement({
                        effect: iam.Effect.ALLOW,
                        actions: [
                            "dynamodb:BatchGetItem",
                            "dynamodb:GetRecords",
                            "dynamodb:GetShardIterator",
                            "dynamodb:Query",
                            "dynamodb:GetItem",
                            "dynamodb:Scan",
                            "dynamodb:BatchWriteItem",
                            "dynamodb:PutItem",
                            "dynamodb:UpdateItem",
                            "dynamodb:DeleteItem",
                            "dynamodb:DescribeTable"
                        ],
                        resources: [`arn:aws:dynamodb:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:table/octagon-*`],
                    }),
                    new iam.PolicyStatement({
                        effect: iam.Effect.ALLOW,
                        actions:  [
                            "ssm:GetParameter",
                            "ssm:GetParameters"
                        ],
                        resources   :   [`arn:aws:ssm:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:parameter/*`],
                    }),
                    new iam.PolicyStatement({
                        effect: iam.Effect.ALLOW,
                        actions:  [
                            "sqs:ChangeMessageVisibility",
                            "sqs:SendMessage",
                            "sqs:ReceiveMessage",
                            "sqs:DeleteMessage",
                            "sqs:GetQueueAttributes",
                            "sqs:ListQueues",
                            "sqs:GetQueueUrl",
                            "sqs:ListDeadLetterSourceQueues",
                            "sqs:ListQueueTags"
                        ],
                        resources   :  [`arn:aws:sqs:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:${this.prefix}-${this.team}-*`],
                    }),
                    new iam.PolicyStatement({
                        effect: iam.Effect.ALLOW,
                        actions:  [
                            "states:StartExecution"
                        ],
                        resources   :   [
                            `arn:aws:states:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:stateMachine:${this.prefix}*`
                        ],
                    }),
                    new iam.PolicyStatement({
                        effect: iam.Effect.ALLOW,
                        actions: [
                            "s3:Get*",
                            "s3:List*",
                            "s3-object-lambda:Get*",
                            "s3-object-lambda:List*"
                        ],
                        resources: ["*"],
                    }),
                    new iam.PolicyStatement({
                        effect: iam.Effect.ALLOW,
                        actions: [
                            "sqs:SendMessage",
                            "sqs:GetQueueAttributes",
                            "sqs:ListQueues",
                            "sqs:GetQueueUrl",
                            "sqs:ListDeadLetterSourceQueues",
                            "sqs:ListQueueTags"
                        ],
                        resources: [`arn:aws:sqs:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:${this.prefix}-${this.team}-*`],
                    }),
                    new iam.PolicyStatement({
                        effect: iam.Effect.ALLOW,
                        actions: [
                            "states:StartExecution"
                        ],
                        resources: [
                            `arn:aws:states:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:stateMachine:${this.prefix}*`
                        ],
                    })
                ]
            }),
        })

        this.config.rawBucketKey.grantDecrypt(role)
        this.config.rawBucket.grantRead(role)
        this.config.stageBucketKey.grantEncrypt(role)
        this.config.stagebucket.grantWrite(role)
        this.routingQueue.grantSendMessages(role)
        this.routingQueue.grantConsumeMessages(role)
        this.routingDLQ.grantSendMessages(role)
        this.routingDLQ.grantConsumeMessages(role)
        this.sqsKey.grantEncryptDecrypt(role)

        return role
    }
    protected createLambdaFunction(
        stepName: string,
        memorySize?: number,
        timeout?: number,
    ): lambda.IFunction {
        return new lambda.Function(
            this,
            `${this.prefix}-${this.team}-${this.pipeline}-${stepName}`,
            {
                functionName: `${this.prefix}-${this.team}-${this.pipeline}-${stepName}-a`,
                code: lambda.Code.fromAsset(
                    `datalake/src/lambdas/sdlflighttransform/${stepName}`
                ),
                handler: "handler.lambdahandler",
                environment: {
                    "stagebucket": `${this.prefix}-${this.environmentId}-${cdk.Aws.REGION}-${cdk.Aws.ACCOUNT_ID}-stage`,
                    "TEAM": this.team,
                    "PIPELINE": this.pipeline,
                    "STAGE": "StageA"
                },
                role: this.lambdaRole,
                description: `execute ${stepName} step of light transform`,
                timeout: cdk.Duration.minutes(timeout ?? 15),
                memorySize: memorySize,
                runtime: this.config.runtime,
                layers: [this.config.wranglerLayer, this.config.dataLakeLib]
            }
        )
    }
    protected createLambdaTask(stepName: string, resultPath?: string, memorySize?: number): tasks.LambdaInvoke { 

        return new tasks.LambdaInvoke(
            this,
            `${this.prefix}-${this.team}-${this.pipeline}-${stepName}-task`,
            {
                lambdaFunction: this.createLambdaFunction(stepName, memorySize),
                resultPath: resultPath
            }
        )
    }
    protected buildStateMachine(
        preupdateTask: tasks.LambdaInvoke,
        processTask: tasks.LambdaInvoke,
        postupdateTask: tasks.LambdaInvoke,
        errorTask: tasks.LambdaInvoke,
    ): void {
        // Success/Failure States
        const successState = new sfn.Succeed(this, `${this.prefix}-${this.team}-${this.pipeline}-success`)
        const failState = new sfn.Fail(this, `${this.prefix}-${this.team}-${this.pipeline}-fail`, {error: "States.ALL"})

        // CREATE PARALLEL STATE DEFINITION
        const parallelState = new sfn.Parallel(this, `${this.prefix}-${this.team}-${this.pipeline}-ParallelSM-A`)

        parallelState.branch(
            preupdateTask
            .next(processTask)
            .next(postupdateTask)
        )

        parallelState.next(successState)

        parallelState.addCatch(
            errorTask,
            {
                errors: ["States.ALL"],
                resultPath: sfn.JsonPath.DISCARD
            }
        )

        errorTask.next(failState)

        this.createStateMachine(
            parallelState,
            {
                additionalRolePolicyStatements: [
                    new iam.PolicyStatement({
                        effect: iam.Effect.ALLOW,
                        actions: [
                            "lambda:InvokeFunction"
                        ],
                        resources: [
                            `arn:aws:lambda:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNTID}:function:${this.prefix}-${this.team}-${this.pipeline}-*`
                        ],
                    })
                ]
            }
        )
        new ssm.StringParameter(
            this,
            `${this.prefix}-${this.team}-${this.pipeline}-state-machine-a-ssm`,
            {
                parameterName: `/SDLF/SM/${this.team}/${this.pipeline}StageASM`,
                stringValue: this.stateMachine.stateMachineArn,
            }
        )
    }
}
