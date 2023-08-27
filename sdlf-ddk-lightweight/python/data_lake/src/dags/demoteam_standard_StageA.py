import ast
import boto3
from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator                                                                
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

# Define the DAG
dag = DAG(
    'demoteam_standard_StageA',
    default_args=default_args,
    description='Stage-A airflow dag to invoke AWS Lambda functions',
    schedule_interval=None,
    render_template_as_native_obj=True,
)


# Function to invoke AWS Lambda
def invoke_lambda_function(step_name, **context):
    
    template =  context['templates_dict']
    event = template.get("event")
    additional_event = template.get("additional_body", [])
    
    prefix = context['dag_run'].conf.get('prefix', "")
    team = context['dag_run'].conf.get('team', "")
    pipeline = context['dag_run'].conf.get('pipeline', "")
    
    for ae in additional_event:
        event["Payload"]["body"].update({ "processedKeys":{ "Payload": ae }  })
    
    lambda_client = boto3.client('lambda')
    
    # Invoke the Lambda function
    response = lambda_client.invoke(
        FunctionName=f"{prefix}-{team}-{pipeline}-{step_name}-a",
        Payload=json.dumps(event),
        InvocationType='RequestResponse'
    )
    
    return response["Payload"].read().decode("utf-8")

# Step 1: Invoke Preupdate Lambda function 
preupdate_task = PythonOperator(
    task_id='preupdate',
    python_callable=invoke_lambda_function,
    op_kwargs={"step_name":"preupdate"},  
    dag=dag,
    provide_context=True,
    templates_dict={"event": "{{ dag_run.conf | tojson}}" }
)

# Step 2: Invoke Process Lambda function
process_task = PythonOperator(
    task_id='process',
    python_callable=invoke_lambda_function,
    op_kwargs={"step_name":"process"},  
    dag=dag,
    provide_context=True,
    templates_dict={"event": {"Payload": "{{ ti.xcom_pull(task_ids='preupdate') }}" }}
)

# Step 3: Invoke Postupdate Lambda function
postupdate_task = PythonOperator(
    task_id='postupdate',
    python_callable=invoke_lambda_function,
    op_kwargs={"step_name":"postupdate"},  
    dag=dag,
    provide_context=True,
    templates_dict={"event":{"Payload": "{{ ti.xcom_pull(task_ids='preupdate') }}" }, "additional_body" : [ "{{ ti.xcom_pull(task_ids='process') }}" ]    }
)


# Define the task dependencies
preupdate_task >> process_task >> postupdate_task
