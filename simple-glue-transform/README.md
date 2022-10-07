# Simple Glue Job Transform with AWS DataOps Development Kit

In this DDK example, an S3 *"Object Created"* event triggers a Glue Job defined by the user.

## Walkthrough

To use the example, clone the repo:

```console
git clone https://github.com/aws-samples/aws-ddk-examples.git
```

Next, navigate into the example directory, and create a virtual environment:

```console
cd simple-glue-transform && python3 -m venv .venv
```

To activate the virtual environment, and install the dependencies, run:

```console
source .venv/bin/activate && pip install -r requirements.txt
```

If your AWS account hasn't been used to deploy DDK apps before, then you must bootstrap your environment first:

```console
ddk bootstrap -e dev # This example uses an environment titled 'dev'
```

You can then deploy your DDK app:

```console
ddk deploy
```
