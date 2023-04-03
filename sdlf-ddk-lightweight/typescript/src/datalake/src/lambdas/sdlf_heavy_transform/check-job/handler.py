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
from datalake_library.transforms.transform_handler import TransformHandler
from datalake_library import octagon
from datalake_library.octagon import Artifact, EventReasonEnum, peh

logger = init_logger(__name__)


def lambda_handler(event, context):
    """Calls custom job waiter developed by user

    Arguments:
        event {dict} -- Dictionary with details on previous processing step
        context {dict} -- Dictionary with details on Lambda context

    Returns:
        {dict} -- Dictionary with Processed Bucket, Key(s) and Job Details
    """
    try:
        logger.info('Fetching event data from previous step')
        bucket = event['body']['bucket']
        keys_to_process = event['body']['keysToProcess']
        team = event['body']['team']
        stage = event['body']['pipeline_stage']
        dataset = event['body']['dataset']
        job_details = event['body']['job']['Payload']['jobDetails']
        processed_keys_path = event['body']['job']['Payload']['processedKeysPath']

        logger.info('Initializing Octagon client')
        component = context.function_name.split('-')[-2].title()
        octagon_client = (
            octagon.OctagonClient()
            .with_run_lambda(True)
            .with_configuration_instance(event['body']['env'])
            .build()
        )

        logger.info('Checking Job Status with user custom code')
        transform_handler = TransformHandler().stage_transform(team, dataset, stage)
        response = transform_handler().check_job_status(bucket, keys_to_process,
                                                        processed_keys_path, job_details)  # custom user code called
        response['peh_id'] = event['body']['job']['Payload']['peh_id']

        if event['body']['job']['Payload']['jobDetails']['jobStatus'] == 'FAILED':
            peh.PipelineExecutionHistoryAPI(
                octagon_client).retrieve_pipeline_execution(response['peh_id'])
            octagon_client.end_pipeline_execution_failed(component=component,
                                                         issue_comment="{} {} Error: Check Job Logs".format(stage, component))
    except Exception as e:
        logger.error("Fatal error", exc_info=True)
        peh.PipelineExecutionHistoryAPI(octagon_client).retrieve_pipeline_execution(
            event['body']['job']['Payload']['peh_id'])
        octagon_client.end_pipeline_execution_failed(component=component,
                                                     issue_comment="{} {} Error: {}".format(stage, component, repr(e)))
        raise e
    return response
