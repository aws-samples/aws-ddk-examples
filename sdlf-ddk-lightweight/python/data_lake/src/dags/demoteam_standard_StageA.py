import ast
import boto3
from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.amazon.aws.operators.lambda_function import AwsLambdaInvokeFunctionOperator                                                                  
import logging
import json
logger = logging.getLogger()
logger.setLevel(logging.INFO)
# Import any other required libraries for AWS Lambda invocation here

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'demoteam_standard_StageA',
    default_args=default_args,
    description='An Airflow DAG to invoke AWS Lambda functions',
    schedule_interval=None,
)


# Function to invoke AWS Lambda
def invoke_lambda_function(task_id, function_name, **context):
    
    template =  context['templates_dict']
    event = {}
    event['Payload'] = json.loads(template.get("Payload", '{}'))
    additional_event = template.get("additional_payload", [])
    
    for ae in additional_event:
        event["Payload"]["body"].update({ "processedKeys":{ "Payload": ast.literal_eval(ae) }  })
    
    lambda_client = boto3.client('lambda')
    
    # Invoke the Lambda function
    response = lambda_client.invoke(
        FunctionName=function_name,
        Payload=json.dumps(event),
        InvocationType='RequestResponse'
    )

    return json.loads(response['Payload'].read().decode("utf-8"))

# Step 1: Invoke Lambda 1
preupdate_task = AwsLambdaInvokeFunctionOperator(
        task_id='preupdate',
        function_name='sdlf-demoteam-standard-preupdate-a',
        payload="{{ dag_run.conf | tojson}}",  # Pass context as payload
        dag=dag
    )

# Step 2: Invoke Lambda 2
process_task = PythonOperator(
    task_id='process',
    python_callable=invoke_lambda_function,
    op_args=['process',"sdlf-demoteam-standard-process-a"],  # Replace with your Lambda function name
    dag=dag,
    provide_context=True,
    templates_dict={"Payload": "{{ ti.xcom_pull(task_ids='preupdate') }}" }
)

# Step 3: Invoke Lambda 3
postupdate_task = PythonOperator(
    task_id='postupdate',
    python_callable=invoke_lambda_function,
    op_args=['postupdate',"sdlf-demoteam-standard-postupdate-a"],  # Replace with your Lambda function name
    dag=dag,
    provide_context=True,
    templates_dict={"Payload": "{{ ti.xcom_pull(task_ids='preupdate') }}" , "additional_payload" : [ "{{ ti.xcom_pull(task_ids='process') }}" ]    }
)


# Define the task dependencies
preupdate_task >> process_task >> postupdate_task
