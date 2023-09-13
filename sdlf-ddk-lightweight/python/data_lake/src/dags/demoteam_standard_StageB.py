import ast
import time
import boto3
from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator        
from airflow.exceptions import AirflowFailException                                                        
import logging
import json
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}



def check_job(step_name, event, additional_body= [],  **context):
    ''' Python function to check status of Glue Job through a lambda function
        Args:
            step_name: step name
            event: airflow dag trigger event
            additional_body: list of dict to be combined with event body
        Returns:
            the response for check-job lambda function
        Raises:
            AirflowFailException: if status from check-job lambda function is FAILED
        
    '''
    status = additional_body[0]["jobDetails"]["jobStatus"]

    while status not in ["SUCCEEDED", "FAILED"] :
        logger.info("waiting for 5 mins...")
        time.sleep(300) 

        response = invoke_lambda_function(step_name, event, additional_body, **context )
        response = json.loads(response)
        status = response["jobDetails"]["jobStatus"]
        additional_body = [response]
        
    if status == "SUCCEEDED":
        return response
    else: 
        raise AirflowFailException(response)
    

# error handling when dag fails
def invoke_error_lambda(context):
    ''' In case when dag fails, this function triggers the error lambda function 
        Args:
            context: Airflow dag context
        Returns:
            
        Raises:
        
    '''
    lambda_client = boto3.client('lambda')

    event = context['dag_run'].conf
    
    logger.info("Invoking Error lambda function")
    lambda_client.invoke(
        FunctionName=f"{event['prefix']}-{event['body']['team']}-{event['body']['pipeline']}-error-b",
        Payload=json.dumps(event),
        InvocationType='RequestResponse'  
    )
    logger.error(event)
    

# Function to invoke AWS Lambda
def invoke_lambda_function(step_name, event, additional_body= [],  **context):
    ''' In case when dag fails, this function triggers the error lambda function 
        Args:
            step_name: step name
            event: airflow dag trigger event
            additional_body: list of dict to be combined with event body
            context: Airflow dag context
        Returns:
            response from lambda function
        Raises:
            AirflowFailException: in case of error or if errorMessage in lambda response
        
    '''
    event_body = event["body"]
    prefix = event.get('prefix', "")
    team = event_body.get('team', "")
    pipeline = event_body.get('pipeline', "")
    stage = event["body"]["pipeline_stage"]
    for ae in additional_body:
        event["body"].update({ "job": {"Payload": ae }  } )
    
    lambda_client = boto3.client('lambda')
    
    # Invoke the Lambda function
    response = lambda_client.invoke(
        FunctionName=f"{prefix}-{team}-{pipeline}-{step_name}-{stage[-1].lower()}",
        Payload=json.dumps(event),
        InvocationType='RequestResponse'
    )
    response = response["Payload"].read().decode("utf-8")
    
    # check if there is an error while processing the files 
    if 'errorMessage' in response:
        raise AirflowFailException(response)
    
    return response


# Define the DAG
dag = DAG(
    'demoteam_standard_StageB',
    default_args=default_args,
    description='Stage-B airflow dag to invoke AWS Lambda functions',
    schedule_interval=None,
    render_template_as_native_obj=True,
    
)


# Step 1: Invoke Process Lambda function 
process_task = PythonOperator(
    task_id='process',
    python_callable=invoke_lambda_function,
    op_kwargs={"step_name":"process", "event": "{{ dag_run.conf | tojson}}" },  
    dag=dag,
    provide_context=True,
    on_failure_callback = invoke_error_lambda,
)


# Step 3: Invoke Process Lambda function
check_task = PythonOperator(
    task_id='check-job',
    python_callable=check_job,
    op_kwargs={"step_name":"check-job", "event": "{{ dag_run.conf | tojson}}" , 
               "additional_body" : [ "{{ ti.xcom_pull(task_ids='process') }}"  ] },   
    dag=dag,
    provide_context=True,
    on_failure_callback = invoke_error_lambda,
)


# Step 4: Invoke Postupdate Lambda function
postupdate_task = PythonOperator(
    task_id='postupdate',
    python_callable=invoke_lambda_function,
    op_kwargs={"step_name":"postupdate",  "event": "{{ dag_run.conf | tojson}}" , 
               "additional_body" : [ "{{ ti.xcom_pull(task_ids='check-job') }}"  ]},  
    dag=dag,
    provide_context=True,
    on_failure_callback = invoke_error_lambda,
)


# Define the task dependencies
process_task >> check_task
check_task >> postupdate_task

