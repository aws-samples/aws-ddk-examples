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

import os
import shutil

from datalake_library.commons import init_logger
from datalake_library.transforms.transform_handler import TransformHandler
from datalake_library import octagon
from datalake_library.octagon import Artifact, EventReasonEnum, peh

logger = init_logger(__name__)


def lambda_handler(event, context):
    """Calls custom transform developed by user

    Arguments:
        event {dict} -- Dictionary with details on previous processing step
        context {dict} -- Dictionary with details on Lambda context

    Returns:
        {dict} -- Dictionary with Processed Bucket and Key(s)
    """
    try:
        logger.info('Fetching event data from previous step')
        event = event["Payload"]
        bucket = event['body']['bucket']
        key = event['body']['key']
        team = event['body']['team']
        stage = event['body']['pipeline_stage']
        dataset = event['body']['dataset']

        logger.info('Initializing Octagon client')
        component = context.function_name.split('-')[-2].title()
        octagon_client = (
            octagon.OctagonClient()
            .with_run_lambda(True)
            .with_configuration_instance(event['body']['env'])
            .build()
        )
        peh.PipelineExecutionHistoryAPI(
            octagon_client).retrieve_pipeline_execution(event['body']['peh_id'])

        # Call custom transform created by user and process the file
        logger.info('Calling user custom processing code')
        transform_handler = TransformHandler().stage_transform(team, dataset, stage)
        response = transform_handler().transform_object(
            bucket, key, team, dataset)  # custom user code called
        octagon_client.update_pipeline_execution(status="{} {} Processing".format(stage, component),
                                                 component=component)
    except Exception as e:
        logger.error("Fatal error", exc_info=True)
        octagon_client.end_pipeline_execution_failed(component=component,
                                                     issue_comment="{} {} Error: {}".format(stage, component, repr(e)))
        raise e
    return response