# DDK Serverless Data Lake Framework

This codebase is a platform that leverages the [Serverless Data Lake Framework](https://github.com/awslabs/aws-serverless-data-lake-framework/) (SDLF) and [AWS DataOps Development Kit](https://awslabs.github.io/aws-ddk/) (DDK) to accelerate the delivery of enterprise data lakes on AWS.

<br />

**Purpose**

The Purpose of the DDK Serverless Data Lake Framework is to shorten the deployment time to production from several months to a few weeks. Refactoring SDLF on top of the AWS DDK framework allows for more flexible use cases, further customization, and higher level abstraction to reduce the learning curve and make data lakes easier to deploy.

<br />

**Contents:**

- [Reference Architecture](#reference-architecture)
- [Prerequisites for the Deployment](#prequisites-for-the-deployment)
- [AWS Service Requirements](#aws-service-requirements)
- [Deployment](#deployment)
- [Parameters](#parameters)
- [Resources Deployed](#resources-deployed)

<br />
<br />

## Reference Architecture

The reference code in this codebase provides two pipelines to illustrate how teams can author and use new pipelines. The first pipeline is the `StandardPipeline` whose architecture can be seen below. The default values in `parameters.json` deploys a single instance of this pipeline with one team (`"demoteam"`) and one dataset (`"legislators"`).

![Alt](docs/images/StandardEndToEnd.png)

In addition, we define a `CustomPipeline` which is just the stage A of the `StandardPipeline`. This pipeline serves to demonstrate how teams can author custom pipelines an include them in the framework. See the [Adding New Pipelines](#adding-new-pipelines) section below for more details.

![Alt](docs/images/CustomEndToEnd.png)

Finally, by adding additional records to the `parameters.json` file you can deploy multiple datasets for multiple teams with multiple pipelines. An example of what this architecture might look like is shown below.

![Alt](docs/images/MultiTeamDeployment.png)

<br />
<br />

## Prerequisites for the Deployment

<br />

To complete this deployment, you'll need the following in your local environment

Programmatic access to an AWS Account

The AWS CLI installed and configured

```
$ aws --version
```

The AWS CDK CLI (version 2.10 and above) installed, to upgrade use `npm install -g aws-cdk`

```
$ cdk --version
```

The Git CLI (version 2.28 and above) installed and configured

```
$ git --version
```

If this is your first time using Git, set your git username and email by running:

```
$ git config --global user.name "YOUR NAME"
$ git config --global user.email "YOU@EMAIL.COM"
```

You can verify your git configuration with

```
$ git config --list
```
<br />
<br />

## AWS Service Requirements

<br />

The following AWS services are required for this utility:

1. [AWS Lambda](https://aws.amazon.com/lambda/)
2. [Amazon S3](https://aws.amazon.com/s3/)
3. [AWS Glue](https://aws.amazon.com/glue/)
4. [Amazon DynamoDB](https://aws.amazon.com/dynamodb/)
5. [AWS Identity and Access Management (IAM)](https://aws.amazon.com/iam/)
6. [Amazon Simple Queue Service](https://aws.amazon.com/sqs/)
7. [AWS Key Management Service (KMS)](https://aws.amazon.com/kms/)
8. [AWS Lake Formation](https://aws.amazon.com/lake-formation/)
9. [AWS EventBridge](https://aws.amazon.com/eventbridge/)

## Deployment

<br />

### Initial setup

Install the dependencies for the project.
This is when the AWS DDK Core library is installed

```
$ npm install
```
<br />
<br />

### CDK Bootstrapping

<br />

If your AWS account/s hasn't been used to deploy DDK apps before, then you need to bootstrap your environment for both the `cicd` and the `dev` environments depending upon if they are same account region or multi account region

If they are same account region

```
$ cdk bootstrap --profile [AWS_PROFILE] or cdk bootstrap aws://ACCOUNT-NUMBER-1/REGION-1
```

If they are different account region 

```
$ cdk bootstrap --profile [AWS_PROFILE_DEV] or cdk bootstrap aws://ACCOUNT-NUMBER-1/REGION-1
$ cdk bootstrap --profile [AWS_PROFILE_CICD] or cdk bootstrap aws://ACCOUNT-NUMBER-1/REGION-1

or 

$ cdk bootstrap aws://ACCOUNT-NUMBER-1/REGION-1 aws://ACCOUNT-NUMBER-1/REGION-1
```

<br />
<br />

### Adding LakeFormation Data Lake Settings

<br />

In the AWS Console, navigate to AWS Lake Formation. Click on Settings on the left-hand side menu and make sure both boxes are unchecked:

- `Use only IAM access control for new databases`
- `Use only IAM access control for new tables in new databases`

Click the `Save` button on the bottom right-hand side of the page when done.

Next, click on `Administrative roles and tasks` on the left-hand side menu and click on `Choose Administrators`. Add both:

- Your current IAM role as a Data Lake administrator
- The `CloudFormationExecutionRole` IAM role name returned by the DDK Bootstrap Stack for the environment you are deploying to (e.g. `cdk-hnb659fds-cfn-exec-ACCOUNTID-REGION`)

<br />
<br />

### Configure ddk.json 

<br />

You might recognize a number of files typically found in a CDK Python application (e.g. app.py, cdk.json...). In addition, a file named `ddk.json` holding configuration about DDK specific constructs is present.

Before deploying the pipeline, edit the `ddk.json` file for the environment you are deploying to with:

1. The correct AWS Account ID in `account` field
2. The region to deploy the DDK SDLF Example in the `region` field
3. Set cicd_enabled to `true` or `false` depending on your usecase


Additionally, edit the `parameters.json` file under the path `data_lake/pipelines/parameters.json` with the correct:

1. `team` — The name of the team which owns the pipeline.
2. `pipeline` — Name to give the pipeline being deployed.
3. `dataset` - Name of the dataset being deployed.
4. `stage_a_transform` - Name of the python file containing light transform code.
5. `stage_b_transform` - Name of the python file containing heavy transform code.

If the parameters are not filled, default values for team, pipeline, dataset, stage_a_transform, and stage_b_transform will be used (see Reference Architecture section above).

<br />
<br />

### Performing Git Operations [Only for CICD enabled = `true` option]
<br />

Initialise git for the repository

```
$ git init --initial-branch main
```

Execute the create repository command to create a new codecommit repository in the aws account

_NOTE: Make Sure the REPO_NAME matches the repository name value in the `cicd` environment of the `ddk.json` file or vice versa before executing_

```
$ aws codecommit create-repository --repository-name REPO_NAME --profile [AWS_PROFILE] --region [AWS_REGION]
```

Add and push the initial commit to the repository

```
$ git remote add origin {URL}
$ git add .
$ git commit -m "Configure SDLF DDK"
$ git push --set-upstream origin main
```
<br />
<br />

### Deploying SDLF

<br />

Once the above steps are performed, verify the below and run the deploy command to deploy SDLF

1.  `ddk.json` file is updated with required configuration
2.  `parameters.json` file is updated if required

```shell
# FOR CICD
$ cdk deploy --profile [AWS_PROFILE_CICD]

# FOR NO CICD
$ cdk deploy --all --profile [AWS_PROFILE]
```

Depending on CICD or not, it deploys all step deploys an AWS CICD CodePipeline along with its respective AWS CloudFormation Stacks. The last stage of each pipeline delivers the SDLF Data Lake infrastructure respectively in the child (default dev) environment through CDK/CloudFormation. For Non CICD deployment, it will directly deploy all SDLF components.

*When you do the initial deployment in a brand new account, you may encounter some transient errors in CodePipeline or CodeBuild as it may take some time (typically a couple of hours) for AWS to provision your capacity in those services. If this happens you can wait some time and click the Retry button on CodePipeline to retry a failed stage.*

<br />
<br />

---

### Datasets

<br />

A dataset is a logical construct referring to a grouping of data. It can be anything from a single table to an entire database with multiple tables, for example. However, an overall good practice is to limit the infrastructure deployed to the minimum to avoid unnecessary overhead and cost. It means that in general, the more data is grouped together the better. Abstraction at the transformation code level can then help make distinctions within a given dataset.

Examples of datasets are:

- A relational database with multiple tables (E.g. Sales DB with orders and customers tables)
- A group of files from a data source (E.g. XML files from a Telemetry system)
- A streaming data source (E.g. Kinesis data stream batching files and dumping them into S3)

For this QuickStart, a Glue database alongside Lake Formation permissions are created for each dataset. However, this will depend on the use case with the requirements for unstructured data or for a streaming data source likely to be different. The structure of the dataset, and what infrastructure is deployed with it, depend on the pipeline it belongs to.

<br />
<br />

---

### Pipelines

<br />

A data pipeline can be thought of as a logical construct representing an ETL process that moves and transforms data from one area of the lake to another. The pipelines directory is where the blueprints for the data pipelines, their stages, and their datasets are defined by data engineers. In addition, generic stages to be used across multiple pipelines can be defined in the pipelines/common_stages directory. For instance, the definition for a step function stage orchestrating a Glue job and updating metadata is abstracted in the `sdlf_heavy_transform.py` file within the common_stages directory. This definition is not specific to a particular job or crawler, instead the Glue job name is passed as an input to the stage. Such a configuration promotes the reusability of stages across multiple data pipelines.

In the pipelines directory, these stage blueprints are instantiated and wired together to create a data pipeline. Borrowing an analogy from object-oriented programming, blueprints defined in the stages directory are “classes”, while in the pipelines directory these become “object instances” of the class.

<br />
<br />

---

### Data Lake Processing sample data

<br />

- If you are using this in a burner account or for demos, you can use the demo data and default `data_lake_parameters` in `ddk.json` to test the data lake functionality.

- Execute the below command with the necessary details for the variables such as `BUCKET_NAME`, `PROFILE` and `REGION`. Once executed it will put the data in respective s3 bucket and start data lake processing. You can update the file to also copy other sample data, or change the `DATASET` and `TEAM` parameters to test multiple pipelines.

```shell
$ sh ./examples/copy.sh BUCKET_NAME REGION PROFILE
```

<br />
<br />

---

### Adding New Datasets with Custom Transformations in the Same Pipeline

<br />

*Remember if using typescript for CDK infrastructure-as-code, the SDLF library is still written in **python** and all custom transformation code will need to be* **python**

1. For the `parameters.json` file located at the path `data_lake/pipelines/`, specify a new dictionary for the creation of a new dataset, for example:

```json
{
    "dev": [
        {
            "team": "demoteam",
            "pipeline": "standard",
            "dataset": "legislators",
            "config":{
                "stage_a_transform": "sdlf_light_transform",
                "stage_b_transform": "sdlf_heavy_transform"
            }
        },
        {
            "team": "demoteam",
            "pipeline": "standard",
            "dataset": "newdata",
            "config": {
                "stage_a_transform": "sdlf_light_transform_new",
                "stage_b_transform": "sdlf_heavy_transform_new"
            }
        }
    ]
}
```

2. Create new transformation code for the new dataset for both the Stage A and Stage B step functions to use to process your data being ingested (the defaults one created are `sdlf_light_transform.py` and `sdlf_heavy_transform.py`). Add the transformation code to the following paths for light and heavy transforms respectively:

```shell
data_lake/src/layers/data_lake_library/python/datalake_library/transforms/stage_a_transforms/sdlf_light_transform.py

data_lake/src/layers/data_lake_library/python/datalake_library/transforms/stage_b_transforms/sdlf_heavy_transform.py
```

*Use the `sdlf_light_transform.py` and `sdlf_heavy_transform.py` as reference for how to structure the file. The names of the files should match the names of the `stage_a_transform` and `stage_b_transform` specified in the `parameters.json` above.*

3. Add a new python script for a Glue Job specific to this dataset under the path:

```shell
data_lake/src/glue/pyshell_scripts/sdlf_heavy_transform/<TEAM_NAME>/<DATASET_NAME>/main.py
```

*The `TEAM_NAME` and `DATASET_NAME` as part of the file path should match the dataset specified in the `parameters.json` above.*

4. Push your code to the remote CodeCommit repository and the DDK SDLF Data Lake will automatically create new resources for your additional dataset.

<br />
<br />

---

### Adding New Pipelines

<br />

**If you want to provide different step machines that what is provided out of the box for DDK SDLF, follow the steps below:**

1. In the `data_lake/pipelines/parameters.json` file, specify a new dictionary for the creation of a new pipeline, for example:

```json
{
    "dev": [
        {
            "team": "demoteam",
            "pipeline": "standard",
            "dataset": "legislators",
            "config": {
                "stage_a_transform": "sdlf_light_transform",
                "stage_b_transform": "sdlf_heavy_transform"
            }
        },
        {
            "team": "demoteam",
            "pipeline": "main",
            "dataset": "newdata",
            "config": {
                "stage_a_transform": "main_light_transform",
                "stage_b_transform": "main_heavy_transform"
            }
        }
    ]
}
```

2. Follow the steps for [adding new datasets with custom transformation](#adding-new-datasets-with-custom-transformations-in-the-same-pipeline) above for the new dataset specified in the `parameters.json`

*Ensure that the `dataset` name does NOT exist in other pipelines for the same team.*

3. Adding new Custom Stage to a Pipeline

---

- Determine what the new pipeline must look like and if you can re-use existing stages or require new custom stages.

- Add a folder to the pipelines directory for your custom pipeline, e.g. "pipelines/main_pipeline/".

- If required to have custom stages, create new files in the folder for your pipeline with the custom stages you wish to have for your data processing pipeline by using the default `src/datalake/pipelines/common-stages/sdlf-light-transform.ts` and `src/datalake/pipelines/common-stages/sdlf-heavy-transform.ts` files as reference.

    - The default stages `sdlf-light-transform.ts` and `sdlf-heavy-transform.ts` include lambdas, queues and step function definitions for your pipeline written as CDK and DDK Constructs

    -  Ensure the custom stages have the properties `targets`, `stateMachine`, and `eventPattern`:

    ```typescript
    readonly targets?: events.IRuleTarget[];
    readonly eventPattern?: events.EventPattern;
    readonly stateMachine: sfn.StateMachine;

    constructor(scope: Construct, id: string, props: MainPipelineProps) {
        super(scope, id, props);

        this.targets = [...];
        this.stateMachine = ...;
        this.eventPattern = ...;
    }

- Add the stages in the `index.ts` file of your custom pipeline directory.

---

4. Creating a new Custom Pipeline

- Add TypeScruot files to define both your pipeline and it's associated dataset to your custom pipeline directory. You can use the `pipelines/standard-pipeline/standard-pipeline.ts` and `pipelines/standard-pipeline/standard-dataset-stack.ts` as examples.

    - You should define the base, shared (across datasets) infrastructure of the pipeline, in its constructor method.

    - Each pipeline must implement the `SDLFPipeline` protocol defined in `pipelines/sdlf-base-stack.ts`. Specifically, it must provide a `registerDataset(dataset: string, config: StandardDatasetConfig)` function which creates any infrastructure specific to each dataset and registers the dataset to the pipeline (such as creating EventBridge rules for new data arriving in S3 for that dataset). It must also include a `PIPELINE_TYPE` class variable that defines the value used in `parameters.json` to register a dataset to that pipeline.

    - As seen in `standard-pipeline.ts` link the custom stages in your Data Pipeline using the DDK `DataPipeline` class as shown below:

```javascript
this.datalakePipeline = new DataPipeline(this, this.pipelineId, {
      name: `${this.resourcePrefix}-DataPipeline-${this.team}-${PIPELINE_TYPE}-${this.environmentId}`,
      description: `${this.resourcePrefix} data lake pipeline`
    })
      .addStage({ stage: this.s3EventCaptureStage })
      .addStage({ stage: this.datalakeLightTransform, skipRule: true })
      .addStage({ stage: datalakeHeavyTransform, skipRule: true });
```

*You can leverage AWS DDK’s built-in S3 Event Stage to set up event-driven architectures and trigger your data processing pipelines*

- Once you have defined your pipeline and stages, allow teams to use the pipeline by updating the `data_lake/pipelines/sdlf_base_stack.py` similar to the following:

    - Change the code block in the `sdlf_base_stack.py` file to check the `customer_config` for your custom `pipeline_type` and create the pipeline as seen below:

```javascript
customerConfigs[props.environmentId].forEach((customerConfig: any) => {
      const dataset = customerConfig.dataset;
      const team = customerConfig.team;
      const pipelineType = customerConfig.pipeline ?? 'standard';

      const pipelineName = `${team}-${pipelineType}`;
      var pipeline: StandardPipeline | CustomPipeline;
      if (!(pipelineName in pipelines)) {
        if (pipelineType == 'standard') {
          pipeline = new StandardPipeline(this, `${team}-${pipelineType}`, {
            environmentId: props.environmentId,
            resourcePrefix: this.resourcePrefix,
            team: team,
            foundationsStage: this.foundationsStage,
            wranglerLayer: this.wranglerLayer,
            app: this.app,
            org: this.org,
            runtime: lambda.Runtime.PYTHON_3_9
          });
        } else if (pipelineType == 'custom') {
    ...
```


5. Push your code to the remote CodeCommit repository and the DDK SDLF Data Lake will automatically create new resources for your additional pipeline.

<br />
<br />

---

### Cleaning Up SDLF Infrastructure

<br />

Once the solution has been deployed and tested, use the following command to clean up the resources depending if you have deployed with CICD or not

```
$ make delete_all or make delete_all_no_cicd
```

Before running this command, look into the `Makefile` and ensure that:

1.  The `delete_repositories` function is passing the correct `-d REPO_NAME` (default: `sdlf-ddk-example`)

2.  The `delete_bootstrap` or `delete_bootstrap_no_cicd` function is passing the correct `--stack-name BOOTSTRAP_STACK_NAME` (default: `CDKToolkit`)

3.  For Single account deployment, make sure you change the `CICD`, `CHILD` variables to your same respective aws profile name. Also update the `ENV` matching `ddk.json`

This command will use a series of CLI commands and python scripts in order to clean up your AWS account environment.

---

### Optional Utils
<br />

Incase you need synthesised YAML templates of the CDK apps, you can create the below command. Make sure your `cicd_enabled` parameter is set in ddk.json

```shell
make cdk_synth_to_yaml
```

Based on cicd_enabled you will get bunch of YAML templates under a new subdirectory `YAML/`

*We recommend to use CDK directly for deployment for better customer experience instead of synthesized YAML template. Deployment and test of YAML template directly to CFN is at your own discretion.*

---