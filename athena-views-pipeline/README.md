# Serverless Athena Views Data Pipeline

This pattern deploys serverless data pipeline that handles Athena Views as part of a step function pipeline on a scheduled event pattern, it also provides a stage to handle error for operations/failure analytics. The code leverages the [AWS DataOps Development Kit (DDK)](https://awslabs.github.io/aws-ddk/) to deploy the infrastructure.
# Architecture
The instructions in this readme will help you deploy the following AWS architecture:

![Screenshot](./docs/athena-views-arch.png)

Feel free to dive into the DDK file, Lambda code if you want to learn more about the implementation details.

# Prerequisites for the Deployment

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

# Initial setup with the DDK CLI

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

# DDK Bootstrapping

In order to deploy DDK apps, you need to bootstrap your environment with the correct environment name.

Run the following command to bootstrap the `dev` environment for your respective AWS Account and Region:

```
$ ddk bootstrap --profile [AWS_PROFILE] --region [AWS_REGION]
```

# Edit DDK.json

Open the `ddk.json` file in the top-level directory of this repo. 

Edit the configuration file and enter your desired **AWS account number** and **AWS region** for the infrastructure to be deployed in.

Be sure to save your changes to the file before proceeding!

# How to define a view?

This pattern contains folders corresponding to databases in glue catalog. In each of these folders, there is one file corresponding to each view available or to be created in glue catalog .

For example, the query which underlies "person_sb"."view_person_gender_male" is located in person_sb/view_person_gender_male.sql.

1. Create a new SQL file whose file name corresponds to the view name
File must be put under the folder which corresponds to the database in the glue catalog where the view will be housed. The filename must be prefixed with view_.

2. Write SQL query
There is no need to write the CREATE OR REPLACE VIEW statement. This will be added automatically, only provide the actual query for the view.

3. you can provide custom cron schedule by adding ```-- SCHEDULE: "cron(*/10 * * * ? *)"``` at the first line of the sql file, if it is not added a default schedule of ```-- SCHEDULE: "cron(*/5 * * * ? *)"``` will be created for that query.

# Deploy the Data Pipeline

To deploy the pipeline, run the following command:

```
$ ddk deploy --profile [AWS_PROFILE]
```

This command should launch a CloudFormation template in the AWS account and region specified in DDK.json, and should take a few minutes to create.

Once the CloudFormation stack has been successfully created, your AWS account now has the data pipeline outlined in the architecture section of this readme! 

# Lakeformation 

If you have enabled lakeformation then you will need to add permissions manually for the roles, in this example, it relies on IAM.
# Testing the Data Pipeline

To test the data pipeline, you will first have to do some pre prep as this example does not provide a pipeline for ingestion and cataloging, it believes such functionality exists within your data lake architecture. It expects the DBs for the views you want to create to already exist

For demo purpose, this example provides some utilities that can be used to ingest an s3 file while simultaneously cataloging in glue catalog using AWS SDK for Pandas (awswrangler)

Once the pre-reqs of DBs and Tables, the rules crated for each db-view will trigger the step function on the defined schedule to create or replace views. You can monitor the step function to see success/failure. if the step function fails, the event is sent to a SqsLambda stage where it puts some important attributes into a DDB table to capture failure metrics

In the command below replace **S3_BUCKET_NAME** with the the name of the S3 bucket created by DDK. 
Also, replace **AWS_PROFILE** with your profile name you have configured for your AWS CLI.

```
$ python utils/example_demo_script.py S3_BUCKET_NAME AWS_PROFILE
```

The above command will place files into S3, create the glue db if it doesnt already exists and catalog the s3 data in glue catalog

*Congrats: You now have tested an operational, Athena Views Data Pipeline built by DDK!*

# Conclusion

This pattern used the DDK to deploy a data pipelines that automates glue catalog view creation for various glue dbs on scheduled pattern using Athena and other AWS analytics services.

In general, this pattern provides the framework for more complex use-cases, while still providing easy-to-use infrastructure by using the DDK!

# Optional: Clean Up 

Enter the S3 bucket name created by this pattern and your AWS CLI profile name, then run the command below to empty the S3 bucket and delete all AWS resources created in this pattern.

```
sh utils/cleanup_script.sh S3_BUCKET_NAME AWS_PROFILE
```
