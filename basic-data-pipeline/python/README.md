# Basic data pipeline example with AWS DataOps Development Kit

In this DDK example, you build a data pipeline that ingests data to S3 using Kinesis Firehose Delivery Stream, and processes it with an AWS Lambda function.

<img align="center" src="docs/_static/basic-data-pipeline.png">

## Walkthrough

Navigate into the example directory, and create a virtual environment:

```console
cd basic-data-pipeline && python3 -m venv .venv
```

To activate the virtual environment, and install the dependencies, run:

```console
source .venv/bin/activate && pip install -r requirements.txt
```

If your AWS account hasn't been used to deploy DDK apps before, then you must bootstrap your environment first:

```console
cdk bootstrap --profile or cdk bootstrap aws://ACCOUNT-NUMBER-1/REGION-1
```

You can then deploy your DDK app:

```console
cdk deploy
```
