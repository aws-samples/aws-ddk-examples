# AWS DDK Examples

This repository contains a list of example projects for the [AWS DataOps Development Kit (DDK)](https://awslabs.github.io/aws-ddk/).

## Table of Contents
1. [About this Repo](#About)
2. [Examples](#Examples)
3. [Learning Resources](#Learning)
4. [License](#License)

## About this Repo <a name="About"></a>
This repository holds our official list of DDK examples code. They aim to showcase the different ways you can leverage the DDK to help with your DataOps workloads, including building a data pipeline to ingest SaaS data or to create your private code artifactory, for example.

We welcome contributions to this repository in the form of enhancements to existing examples or the addition of new ones. For more information on contributing, please see the [CONTRIBUTING](https://github.com/aws-samples/aws-ddk-examples/blob/main/CONTRIBUTING.md) guide.

It is assumed that you are already somewhat familiar with the AWS DDK. If not, we strongly recommend that you go through our [Learning Resources](#Learning) first, in particular the DDK workshop and documentation.

If you would like to start your journey by looking at an example, we recommend you to start with [Kinesis to S3 Data Pipeline](https://github.com/aws-samples/aws-ddk-examples/tree/main/basic-data-pipeline) example from the list below.

## Helper scipt for aws-ddk-examples
Create this optional directory for DDK patterns

```shell
mkdir directory_name
cd directory_name
```
Download the helper script using the below command

```shell
curl -LJO https://raw.githubusercontent.com/aws-samples/aws-ddk-examples/main/cli_helper.py
```

Once the script is downloaded, execute the script using the below command

```shell
pip install urllib3
```

For help regarding usage

```shell
python3 cli_helper.py -h
```

For list existing patterns

```shell
python3 cli_helper.py -t "list"
```

To init an available pattern into your directory

```shell
python3 cli_helper.py -t "init" -p "sdlf-ddk-lightweight" -l "python"
```

> **Earlier Versions of DDK**
> [See here](https://github.com/aws-samples/aws-ddk-examples/tree/0.x.x#readme) for examples using the library before the `1.0.0` major version release.


## Examples <a name="Examples"></a>
| Examples                                                                                                                                    |
|---------------------------------------------------------------------------------------------------------------------------------------------|
| [Kinesis to S3 Data Pipeline](https://github.com/aws-samples/aws-ddk-examples/tree/main/basic-data-pipeline)                                |
| [Google Analytics Appflow Data Pipeline](https://github.com/aws-samples/aws-ddk-examples/tree/main/google-analytics-data-using-appflow)     |
| [Athena Query Execution Pipeline](https://github.com/aws-samples/aws-ddk-examples/tree/main/athena-query-execution-pipeline)                |
| [Athena Views Pipeline](https://github.com/aws-samples/aws-ddk-examples/tree/main/athena-views-pipeline)                                    |
| [DataBrew Athena Pipeline](https://github.com/aws-samples/aws-ddk-examples/tree/main/databrew-athena)                                       |
| [Private Artifactory](https://github.com/aws-samples/aws-ddk-examples/tree/main/private-artifactory)                                        |
| [Cross-Account / Cross-Region Data Pipelines](https://github.com/aws-samples/aws-ddk-examples/tree/main/cross-account-region-data-pipeline) |
| [Data Validation & Cataloging Pipeline](https://github.com/aws-samples/aws-ddk-examples/tree/main/data-validation-cataloging-pipeline)      |
| [SDLF DDK Lightweight](https://github.com/aws-samples/aws-ddk-examples/tree/main/sdlf-ddk-lightweight)                                      |
| [Simple Glue Tranform](https://github.com/aws-samples/aws-ddk-examples/tree/main/simple-glue-transform)                                     |
| [File Standardization Pipeline](https://github.com/aws-samples/aws-ddk-examples/tree/main/file-standardization-pipeline)                    |

## Learning Resources <a name="Learning"></a>
Beyond this repository, there are other resources that can be referenced to assist with your learning/development process.

### Official Resources
- [Documentation](https://awslabs.github.io/aws-ddk/)
- [API Reference](https://awslabs.github.io/aws-ddk/release/stable/api/index)
- [Workshop](https://catalog.us-east-1.prod.workshops.aws/workshops/3644b48b-1d7c-43ef-a353-6edcd96385af/en-US)
- [Source Repository](https://github.com/awslabs/aws-ddk)

# License <a name="License"></a>

This library is licensed under the Apache 2.0 License.
