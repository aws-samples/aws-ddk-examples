# Serverless Athena Query Execution Data Pipeline

## Overview
This pattern deploys serverless data pipelines that handles Athena Query Execution of SQL queries as part of a step function pipeline on a scheduled event pattern which is followed by a glue transform using glue job and crawlers as part of a step function pipeline using a event driven pattern. The code leverages the [AWS DataOps Development Kit (DDK)](https://awslabs.github.io/aws-ddk/) to deploy the infrastructure.


## Architecture
The instructions in this readme will help you deploy the following AWS architecture:

>![Screenshot](./docs/athena-query-execution-arch.png)

Feel free to dive into the DDK file, Glue Script, and Lambda code if you want to learn more about the implementation details.

## Prerequisites for the Deployment

To complete this deployment, you'll need the following in your local environment:

Programmatic access to an AWS Account
Python (version 3.7 or above) and its package manager, pip (version 9.0.3 or above), are required

```
$ python --version
$ pip --version
```

The AWS CLI installed and configured

```
$ aws --version
```

The AWS CDK CLI (version 2.10 and above) installed, to upgrade use `npm install -g aws-cdk`

```
$ cdk --version
```

## Initial setup with the DDK CLI

At this time, you should have downloaded the code for this pattern and should be in the top-level directory of this pattern.

Install AWS DDK CLI, a command line interface to manage your DDK apps

```
$ pip install aws-ddk
```

To verify the installation, run:

```
$ ddk --help
```

Create and activate a virtualenv

```
$ python -m venv .venv && source .venv/bin/activate
```

Install the dependencies from requirements.txt
This is when the AWS DDK Core library is installed

```
$ pip install -r requirements.txt --no-cache-dir
```

If your default AWS region is not set then

```
export AWS_DEFAULT_REGION='<aws-region>'
```

## DDK Bootstrapping

In order to deploy DDK apps, you need to bootstrap your environment with the correct environment name.

Run the following command to bootstrap the `dev` environment for your respective AWS Account and Region:

```
$ ddk bootstrap --profile [AWS_PROFILE] --region [AWS_REGION]
```

## Edit DDK.json

Open the `ddk.json` file in the top-level directory of this repo. 

Edit the configuration file and enter your desired **AWS account number** and **AWS region** for the infrastructure to be deployed in.

Be sure to save your changes to the file before proceeding!

## Edit athena_query_execution/configs.json

Update the json file with the following attributes. see below used as an example. 

```
{
    "dev": [
        {
            "queryId": "query-id1",
            "cronExpression": "cron(*/5 * * * ? *)",
            "queryString": "SELECT minutes_worked, quantity_sold, job_title, sales_person FROM athena_data.sales where quantity_sold > 30 ",
            "table": "sales"
        },
        {
            "queryId": "query-id2",
            "cronExpression": "cron(*/10 * * * ? *)",
            "queryString": "SELECT job_title, sum(quantity_sold) as quantity_sold FROM athena_data.sales GROUP BY job_title ORDER BY quantity_sold DESC;",
            "table": "sales"
        }
    ]
}
```

## Deploy the Data Pipeline

To deploy the pipeline, run the following command:

```
$ ddk deploy --profile [AWS_PROFILE]
```

This command should launch a CloudFormation template in the AWS account and region specified in DDK.json, and should take a few minutes to create.

Once the CloudFormation stack has been successfully created, your AWS account now has the data pipeline outlined in the architecture section of this readme! 

## Testing the Data Pipeline

To test the data pipeline, you will upload a file data.json to S3 using a shell command included in this repo. Within the "utils/data" prefix, the script will upload the data to a top-level prefix to identify the dataset.

For example, the script will upload "data.json" to "s3://DDK_BUCKET_NAME/data/sales/data.json"

If the data pipeline is successful, all of the datasets will be added to the "processed" prefix of the same S3 bucket in parquet format. Scheduled Event will trigger the AthenaSQL Stage step function and run the predefined SQL query which will be stored in "query_output" prefix after which GlueTranform Stage step function will perform addition transformation on the query result data which will be cataloged in another glue db.

For example, the "data.json" dataset should end up in "s3://DDK_BUCKET_NAME/processed/sales/xyz.parquet". Also, the dataset should be added to the Glue Catalog under the "athena_data" Glue Database in a table named "sales". Further, the data will be stored under analytics prefix of the bucket after execution of glue transform stage.

In the command below replace **S3_BUCKET_NAME** with the the name of the S3 bucket created by DDK. 
Also, replace **AWS_PROFILE** with your profile name you have configured for your AWS CLI.

```
$ sh utils/upload_files.sh S3_BUCKET_NAME AWS_PROFILE
```

The above command will place files into S3, and should trigger the data pipeline. The pipeline should take a few minutes to complete, you can use StepFunctions to monitor completion of the Glue tasks.


*Congrats: You now have tested an operational, Athena Query Execution Data Pipeline built by DDK!*

## Conclusion

This pattern used the DDK to deploy multiple automated data pipelines that go through various stages performing usecase based processing showcasing both event-driven and scheduled event patterns using Athena and other AWS analytics services.

The code in this pattern is very generic, and can be extended to include any custom transformations/ data processing that you may need. 

In general, this pattern provides the framework for more complex use-cases, while still providing easy-to-use infrastructure by using the DDK!

## Optional: Clean Up 

Enter the S3 bucket name created by this pattern and your AWS CLI profile name, then run the command below to empty the S3 bucket and delete all AWS resources created in this pattern.

```
sh utils/cleanup_script.sh S3_BUCKET_NAME AWS_PROFILE
```
