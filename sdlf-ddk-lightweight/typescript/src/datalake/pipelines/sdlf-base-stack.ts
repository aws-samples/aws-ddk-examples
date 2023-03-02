import { readFileSync } from "fs";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as ssm from "aws-cdk-lib/aws-ssm";
import { Construct } from "constructs";
import { BaseStack, BaseStackProps } from "aws-ddk-core";

import { FoundationsStack } from "../foundations";
import { StandardPipeline } from "./standard-pipeline";
import { CustomPipeline } from "./custom-pipeline";



// class SDLFPipeline(Protocol):

//     PIPELINETYPE: str

//     def registerdataset(this, dataset: str, config: Dict[str, Any]):
//         ...

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
        const customerConfigs = JSON.parse(readFileSync("./src/pipelines/parameters.json", "utf-8")).environmentId;

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

        //const datasetNames: set[str] = set()
        //const pipelines: Dict[str, SDLFPipeline] = {}
        // # loop through values in parameters.json and create the necessary resources for each pipeline
        // for customerconfig in customerconfigs:
        //     dataset = customerconfig["dataset"]
        //     team = customerconfig["team"]
        //     pipelinetype = customerconfig.get("pipeline", StandardPipeline.PIPELINETYPE)

        //     # PIPELINE CREATION
        //     pipeline: SDLFPipeline
        //     pipelinename = f"{team}-{pipelinetype}"
        //     if pipelinename not in pipelines:
        //         if pipelinetype == StandardPipeline.PIPELINETYPE:
        //             pipeline = StandardPipeline(
        //                 this,
        //                 constructid=f"{team}-{pipelinetype}",
        //                 environmentId=this.environmentId,
        //                 resourcePrefix=this.resourcePrefix,
        //                 team=team,
        //                 foundationsstage=this.foundationsstage,
        //                 wranglerlayer=this.wranglerlayer,
        //                 app=this.app,
        //                 org=this.org,
        //                 runtime=lambda.Runtime.PYTHON39
        //             )
        //         elif pipelinetype == CustomPipeline.PIPELINETYPE:
        //             pipeline = CustomPipeline(
        //                 this,
        //                 constructid=f"{team}-{pipelinetype}",
        //                 environmentId=this.environmentId,
        //                 resourcePrefix=this.resourcePrefix,
        //                 team=team,
        //                 foundationsstage=this.foundationsstage,
        //                 wranglerlayer=this.wranglerlayer,
        //                 app=this.app,
        //                 org=this.org,
        //                 runtime=lambda.Runtime.PYTHON39
        //             )
        //         else:
        //             raise NotImplementedError(
        //                 f"Could not find a valid implementation for pipeline type: {pipelinetype}"
        //             )
        //         pipelines[pipelinename] = pipeline
        //     else:
        //         pipeline = pipelines[pipelinename]

        //     # Register dataset to pipeline with concrete implementations
        //     datasetname = f"{team}-{dataset}"
        //     if datasetname not in datasetnames:
        //         datasetnames.add(datasetname)
        //         pipeline.registerdataset(
        //             dataset,
        //             config=customerconfig.get("config", {})
        //         )





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
                code: lambda.Code.fromasset(
                    os.path.join(
                        f"{Path(file).parents[1]}", "src/layers/datalakelibrary"
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