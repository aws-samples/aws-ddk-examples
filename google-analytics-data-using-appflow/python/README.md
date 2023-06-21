# Analyzing Google Analytics data with Amazon AppFlow, Amazon Athena, and AWS DataOps Development Kit

In this DDK example, you build a data pipeline that ingests Google Analytics data with Amazon AppFlow, processes it with an AWS Lambda function, before analyzing the results with Amazon Athena. It is inspired by this [blog post](https://aws.amazon.com/blogs/big-data/analyzing-google-analytics-data-with-amazon-appflow-and-amazon-athena/).

<img align="center" src="docs/_static/appflow_athena.png">

Note: It is assumed that a [OAuth connection](https://docs.aws.amazon.com/appflow/latest/userguide/google-analytics.html) between Amazon AppFlow and your Google resources was established.

## Walkthrough

Navigate into the example directory, and create a virtual environment:

```console
cd google-analytics-data-using-appflow && python3 -m venv .venv
```

To activate the virtual environment, and install the dependencies, run:

```console
source .venv/bin/activate && pip install -r requirements.txt
```

If your AWS account hasn't been used to deploy DDK apps before, then you must bootstrap your environment first:

```console
$ cdk bootstrap --profile [AWS_PROFILE] or cdk bootstrap aws://ACCOUNT-NUMBER-1/REGION-1
```

Open the `ddk_app/ddk_app_stack.py` file and update relevant values such as the Google Analytics object ID or the SQL query to use in Athena. Note that by default the pipeline is scheduled to ingest data every `1 hour`.

You can then deploy your DDK app:

```console
$ cdk deploy --profile [AWS_PROFILE]
```
