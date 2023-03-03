import * as path from "path";
import { readFileSync } from "fs";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as ssm from "aws-cdk-lib/aws-ssm";
import { Construct } from "constructs";
import { BaseStack, BaseStackProps } from "aws-ddk-core";

import { FoundationsStack } from "../foundations";
import { StandardPipeline } from "./standard-pipeline";
import { CustomPipeline } from "./custom-pipeline";



export interface SDLFBaseStackProps extends BaseStackProps {
    environmentId: string;
    resourcePrefix: string;
    params: any;
}

export class SDLFBaseStack extends BaseStack {
    readonly resourcePrefix: string;
    readonly params: any;
    readonly app: string;
    readonly org: string;
    readonly wranglerLayer: lambda.ILayerVersion;
    readonly foundationsStage: FoundationsStack;

    constructor(scope: Construct, id: string, props: SDLFBaseStackProps) {
        super(scope, id, props);
        this.resourcePrefix = props.resourcePrefix;
        this.params = props.params;
        this.app = this.params.app ?? "datalake";
        this.org = this.params.org ?? "aws";
        const customerConfigs: any = JSON.parse(readFileSync("./src/pipelines/parameters.json", "utf-8"));

        this.wranglerLayer = this.createWranglerLayer()
        this.createDatalakeLibraryLayer()

        // creates DDB tables, routing lambda, data lake buckets,
        // pushes Glue scripts to S3, and creates data lake library lamdba layer
        this.foundationsStage = new FoundationsStack(
            this,
            "foundation-stage",
            {
                environmentId: props.environmentId,
                resourcePrefix: this.resourcePrefix,
                app: this.app,
                org: this.org,
                runtime: lambda.Runtime.PYTHON_3_9
            }
        )

        const datasetNames: any = []
        const pipelines: any = {}
        
        // loop through values in parameters.json and create the necessary resources for each pipeline
        customerConfigs.environmentId.forEach( (customerConfig: any) => {
            const dataset = customerConfig.dataset
            const team = customerConfig.team
            const pipelineType = customerConfig.pipeline ?? "standard"

        // PIPELINE CREATION
            const pipelineName = `${team}-${pipelineType}`
            var pipeline: StandardPipeline | CustomPipeline;
            if (pipelineType == "standard") {
                pipeline = new StandardPipeline(
                    this,
                    `${team}-${pipelineType}`,
                    {
                        environmentId: props.environmentId,
                        resourcePrefix: this.resourcePrefix,
                        team: team,
                        foundationsStage: this.foundationsStage,
                        wranglerLayer: this.wranglerLayer,
                        app: this.app,
                        org: this.org,
                        runtime: lambda.Runtime.PYTHON_3_9
                    }
                )
            }
            else if (pipelineType == "custom"){
                pipeline = new CustomPipeline(
                    this,
                    `${team}-${pipelineType}`,
                    {
                        environmentId: props.environmentId,
                        resourcePrefix: this.resourcePrefix,
                        team: team,
                        foundationsStage: this.foundationsStage,
                        wranglerLayer: this.wranglerLayer,
                        app: this.app,
                        org: this.org,
                        runtime: lambda.Runtime.PYTHON_3_9
                    }
                )
            }
            else {
                throw new Error(`Could not find a valid implementation for pipeline type: ${pipelineType}`);
            }
            pipelines[pipelineName] = pipeline


            // Register dataset to pipeline with concrete implementations
            const datasetName = `${team}-${dataset}`;
            if (!datasetName.includes(datasetName)) {
                datasetNames.add(datasetName)
                pipeline.registerDataset(
                    dataset,
                    customerConfig.config ?? {}
                )
            }
        }
    }
    protected createWranglerLayer(): lambda.ILayerVersion {
        return lambda.LayerVersion.fromLayerVersionArn(
            this,
            "wrangler-layer",
            `arn:aws:lambda:${this.region}:336392948345:layer:AWSDataWrangler-Python39:1`,
        )
    }
    protected createDatalakeLibraryLayer(): void {
        const datalakeLibraryLayer = new lambda.LayerVersion(
            this,
            "data-lake-library-layer",
            {
                layerVersionName: "data-lake-library",
                code: lambda.Code.fromAsset(
                    path.join(
                        __dirname, "src/layers/datalakelibrary"
                    )
                ),
                compatibleRuntimes: [lambda.Runtime.PYTHON_3_9],
                description: `${this.resourcePrefix} Data Lake Library`,
                license: "Apache-2.0",
            }
        )

        new ssm.StringParameter(
            this,
            `data-lake-library-layer-ssm`,
            {
                parameterName: "/SDLF/Layer/DataLakeLibrary",
                stringValue: datalakeLibraryLayer.layerVersionArn,
            }
        )

    }
}