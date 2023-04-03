# Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from datalake_library.commons import init_logger
from datalake_library.configuration.resource_configs import DynamoConfiguration, SQSConfiguration
from datalake_library.interfaces.dynamo_interface import DynamoInterface
from datalake_library.interfaces.s3_interface import S3Interface
from datalake_library.interfaces.sqs_interface import SQSInterface
from datalake_library import octagon
from datalake_library.octagon import Artifact, EventReasonEnum, peh

logger = init_logger(__name__)


def lambda_handler(event, context):
    """Updates the S3 objects metadata catalog

    Arguments:
        event {dict} -- Dictionary with details on Bucket and Keys
        context {dict} -- Dictionary with details on Lambda context

    Returns:
        {dict} -- Dictionary with response
    """
    try:
        logger.info('Fetching event data from previous step')
        bucket = event['body']['bucket']
        processed_keys_path = event['body']['job']['Payload']['processedKeysPath']

        tables_to_process = event['body']['job']['Payload']['jobDetails']['tables']

        table_processed_paths=[]
        for table in tables_to_process:
            path = "{}/{}".format(processed_keys_path, table)
            processed=S3Interface().list_objects(bucket, path)
            for p in processed:
                table_processed_paths.append(p)

        processed_keys = table_processed_paths
        team = event['body']['team']
        pipeline = event['body']['pipeline']
        stage = event['body']['pipeline_stage']
        dataset = event['body']['dataset']
        peh_id = event['body']['job']['Payload']['peh_id']

        logger.info('Initializing Octagon client')
        component = context.function_name.split('-')[-2].title()
        octagon_client = (
            octagon.OctagonClient()
            .with_run_lambda(True)
            .with_configuration_instance(event['body']['env'])
            .build()
        )
        peh.PipelineExecutionHistoryAPI(
            octagon_client).retrieve_pipeline_execution(peh_id)

        logger.info('Initializing DynamoDB config and Interface')
        dynamo_config = DynamoConfiguration()
        dynamo_interface = DynamoInterface(dynamo_config)

        logger.info('Storing metadata to DynamoDB')
        for key in processed_keys:
            object_metadata = {
                'bucket': bucket,
                'key': key,
                'size': S3Interface().get_size(bucket, key),
                'last_modified_date': S3Interface().get_last_modified(bucket, key),
                'org': event['body']['org'],
                'app': event['body']['app'],
                'env': event['body']['env'],
                'team': team,
                'pipeline': pipeline,
                'dataset': dataset,
                'stage': 'stage',
                'pipeline_stage': stage,
                'peh_id': peh_id
            }
            dynamo_interface.update_object_metadata_catalog(object_metadata)

        # Only uncomment if a queue for the next stage exists
        # logger.info('Sending messages to next SQS queue if it exists')
        # sqs_config = SQSConfiguration(team, dataset, ''.join([stage[:-1], chr(ord(stage[-1]) + 1)]))
        # sqs_interface = SQSInterface(sqs_config.get_stage_queue_name)
        # sqs_interface.send_batch_messages_to_fifo_queue(processed_keys, 10, '{}-{}'.format(team, dataset))

        octagon_client.update_pipeline_execution(
            status="{} {} Processing".format(stage, component), component=component)
        octagon_client.end_pipeline_execution_success()
    except Exception as e:
        logger.error("Fatal error", exc_info=True)
        octagon_client.end_pipeline_execution_failed(component=component,
                                                     issue_comment="{} {} Error: {}".format(stage, component, repr(e)))
        raise e
    return 200
