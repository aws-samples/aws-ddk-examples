# Creating private artifactory with AWS CodeArtifact, and AWS DataOps Development Kit

## Overview

This is an example of using [AWS DDK](https://github.com/awslabs/aws-ddk) to create a private artifactory.

In this example you will create Python source code repository, private artifactory, and a continuous 
integration and deployment pipeline that builds, packages, and pushes the artifacts into the artifactory. 
You are now fully equipped to distribute and reuse your Python code across your organization.

<img align="center" src="docs/_static/artifactory.png">

## Walkthrough

To use the example, clone the repo:

```console
git clone https://github.com/aws-samples/aws-ddk-examples.git
```

Next, `cd` into the example directory, and create a virtual environment:

```console
cd private_artifactory && python3 -m venv .venv
```

To activate the virtual environment, and install the dependencies, run:

```console
source .venv/bin/activate && pip install -r requirements.txt
```

If your AWS account hasn't been used to deploy DDK apps before, then you need to bootstrap your environment:

```console
ddk bootstrap
```

You can then deploy your DDK app:

```console
ddk deploy
```

After the app is deployed, the CI/CD pipeline will build and package the artifact. 
You should then be able to log into your artifactory:

```console
aws codeartifact login --tool twine --domain ddk-lib-domain --domain-owner 111111111111 --repository ddk-lib-repository
```

Now you can install the library using PIP and use it as you would use any other Python library:

```console
pip install ddk_lib
```
