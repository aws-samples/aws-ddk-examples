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


import json

from datalake_library.commons import init_logger
from datalake_library.configuration.resource_configs import SQSConfiguration
from datalake_library.interfaces.sqs_interface import SQSInterface

logger = init_logger(__name__)


def lambda_handler(event, context):
    try:
        if isinstance(event, str):
            event = json.loads(event)
        sqs_config = SQSConfiguration(
            event['body']['team'], event['body']['dataset'], event['body']['pipeline_stage'])
        sqs_interface = SQSInterface(sqs_config.get_stage_dlq_name)

        logger.info('Execution Failed. Sending original payload to DLQ')
        sqs_interface.send_message_to_fifo_queue(json.dumps(event), 'failed')
    except Exception as e:
        logger.error("Fatal error", exc_info=True)
        raise e
    return