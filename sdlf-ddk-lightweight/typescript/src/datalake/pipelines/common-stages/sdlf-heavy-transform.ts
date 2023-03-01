import * as cdk from "aws-cdk-lib";
import * as events from "aws-cdk-lib/aws-events";
import * as eventTargets from "aws-cdk-lib/aws-events-targets";
import * as kms from "aws-cdk-lib/aws-kms";
import * as iam from "aws-cdk-lib/aws-iam";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as ssm from "aws-cdk-lib/aws-ssm";
import * as sfn from "aws-cdk-lib/aws-stepfunctions";
import * as tasks from "aws-cdk-lib/aws-stepfunctions-tasks";
import * as cr from "aws-cdk-lib/custom-resources";
import { assignLambdaFunctionProps, StateMachineStage, StateMachineStageProps } from "aws-ddk-core";
import { Construct } from "constructs";

export interface SDLFHeavyTransformConfig {
    team: string;
    pipeline: string;
    stageBucket: s3.IBucket;
    stageBucketKey: kms.IKey;
    datalakelib: lambda.ILayerVersion;
    registerprovider: cr.Provider;
    wranglerlayer: lambda.ILayerVersion;
    runtime: lambda.Runtime;
}

export interface SDLFHeavyTransformProps extends StateMachineStageProps {
    readonly scope: Construct;
    readonly id: string;
    readonly name: string;
    readonly prefix: string;
    readonly environmentId: string,
    readonly config: SDLFHeavyTransformConfig;
}
export class SDLFHeavyTransform extends StateMachineStage {
    readonly targets?: events.IRuleTarget[];
    readonly config: SDLFHeavyTransformConfig;
    readonly environmentId: string;
    readonly prefix: string;
    readonly team: string;
    readonly pipeline: string;
    readonly lambdaRole: iam.Role;
    readonly redriveLambda: lambda.Function;
    readonly routingLambda: lambda.Function;

    constructor(scope: Construct, id: string, props: SDLFHeavyTransformProps) {
        super(scope, id, props);
        this.config = props.config;
        this.environmentId = props.environmentId;
        this.prefix = props.prefix
        this.team = props.config.team
        this.pipeline = props.config.pipeline

        // register heavy transform details in DDB octagon table
        this.registerOctagonConfig()

        // create lambda execution role
        this.lambdaRole = this.createLambdaRole()
        
        // routing functions
        this.redriveLambda = this.createLambdaFunction("redrive")
        this.routingLambda = this.createLambdaFunction("routing")

        // state machine steps
        const processTask = this.createLambdaTask("process", "$.body.job")
        const postupdateTask = this.createLambdaTask("postupdate", "$.statusCode")
        const errorTask = this.createLambdaTask("error")
        const checkJobTask = this.createLambdaTask("check-job", "$.body.job")

        // build state machine
        this.buildStateMachine(processTask, postupdateTask, errorTask, checkJobTask)

        this.targets = new eventTargets.LambdaFunction(this.routingLambda)
    }
    protected registerOctagonConfig(): void {
        const serviceSetupProperties = {"RegisterProperties": json.dumps(this.props)}

        new cdk.CustomResource(
            this,
            `${this.props['id']}-{this.props['type']}-custom-resources   `,
            {
                serviceToken: this.config.registerprovider.serviceToken,
                properties: serviceSetupProperties
            }
        )
    }
    protected createLambdaRole(): iam.IRole {
        const role = new iam.Role(
            this,
            `${this.prefix}-lambda-role-${this.team}-${this.pipeline}-b`,
            {
                roleName: `{this.prefix}-lambda-role-{this.team}-{this.pipeline}-b`,
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
                            "kms:ReEncrypt*"
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
                            "dynamodb:GetItem",
                            "dynamodb:GetRecords",
                            "dynamodb:GetShardIterator",
                            "dynamodb:Query",
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
                    })
                ]
            }),
        })
        this.config.stageBucketKey.grantEncryptDecrypt(role)
        this.config.stageBucket.grantReadWrite(role)

        return role
    }
    protected createLambdaFunction(stepName: string): lambda.IFunction {
        return new lambda.Function(
            this,
            `${this.prefix}-${this.team}-${this.pipeline}-${stepName}-b`,
            assignLambdaFunctionProps(
                {
                    functionName: `${this.prefix}-${this.team}-${this.pipeline}-${stepName}-b`,
                    code: lambda.Code.fromAsset(
                        `datalake/src/lambdas/sdlfheavytransform/${stepName}`
                    ),
                    handler: "handler.lambdahandler",
                    environment: {
                        "TEAM": this.team,
                        "PIPELINE": this.pipeline,
                        "STAGE": "StageB"
                    },
                    role: this.lambdaRole,
                    description: `exeute ${stepName} step of heavy transform.`,
                    timeout: cdk.Duration.minutes(15),
                    memorySize: 256,
                    layers: [this.config.datalakelib, this.config.wranglerlayer],
                    runtime: this.config.runtime,
                }
            )
        )
    }
    protected createLambdaTask(stepname: string, resultpath?: string): tasks.LambdaInvoke { 

        return new tasks.LambdaInvoke(
            this,
            `${this.prefix}-${this.team}-${this.pipeline}-${stepname}-task-b`,
            {
                lambdaFunction: this.createLambdaFunction(stepname),
                resultPath: resultpath
            }
        )
    }
    protected buildStateMachine(
        processTask: tasks.LambdaInvoke,
        postupdateTask: tasks.LambdaInvoke,
        errorTask: tasks.LambdaInvoke,
        checkJobTask: tasks.LambdaInvoke,
    ): void {
        // Success/Failure/Wait States
        const successState = new sfn.Succeed(this, `${this.prefix}-${this.team}-${this.pipeline}-success`);
        const failState = new sfn.Fail(this, `${this.prefix}-${this.team}-${this.pipeline}-fail`, {error: "States.ALL"});
        const jobFailState = new sfn.Fail(
            this,
            `${this.prefix}-${this.team}-${this.pipeline}-job-failed-sm-b`,
            {
                cause: "Job failed, please check the logs",
                error: "Job Failed"
            }
        )
        const waitState = new sfn.Wait(
            this,
            `${this.prefix}-${this.team}-${this.pipeline}-wait-state`,
            {
                time: sfn.WaitTime.duration(cdk.Duration.seconds(15))
            }
        )

        // CREATE PARALLEL STATE DEFINITION
        const parallelState = new sfn.Parallel(this, `${this.prefix}-${this.team}-${this.pipeline}-ParallelSM-B`)

        parallelState.branch(
            processTask
            .next(waitState)
            .next(checkJobTask)
            .next(
                new sfn.Choice(this, `${this.prefix}-${this.team}-${this.pipeline}-is job-complete?`)
                .when(
                    sfn.Condition.stringEquals("$.body.job.Payload.jobDetails.jobStatus", "SUCCEEDED"),
                    postupdateTask
                )
                .when(
                    sfn.Condition.stringEquals("$.body.job.Payload.jobDetails.jobStatus", "FAILED"),
                    jobFailState
                )
                .otherwise(waitState)
            )
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
                            `arn:aws:lambda:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:function:${this.prefix}-${this.team}-${this.pipeline}-*`
                        ],
                    })
                ]
            }
        )
        new ssm.StringParameter(
            this,
            `${this.prefix}-${this.team}-${this.pipeline}-state-machine-b-ssm`,
            {
                parameterName: `/SDLF/SM/${this.team}/${this.pipeline}StageBSM`,
                stringValue: this.stateMachine.stateMachineArn
            }   
        )
    }
}
