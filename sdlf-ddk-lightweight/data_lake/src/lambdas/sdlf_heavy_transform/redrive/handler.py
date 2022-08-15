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
import json

from datalake_library.commons import init_logger
from datalake_library.configuration.resource_configs import SQSConfiguration, StateMachineConfiguration
from datalake_library.interfaces.states_interface import StatesInterface
from datalake_library.interfaces.sqs_interface import SQSInterface

logger = init_logger(__name__)


def lambda_handler(event, context):
    try:
        team = os.environ['TEAM']
        pipeline = os.environ['PIPELINE']
        dataset = event['dataset']
        stage = os.environ['STAGE']
        state_config = StateMachineConfiguration(team, pipeline, stage)
        sqs_config = SQSConfiguration(team, dataset, stage)
        dlq_interface = SQSInterface(sqs_config.get_stage_dlq_name)

        messages = dlq_interface.receive_messages(1)
        if not messages:
            logger.info('No messages found in {}'.format(
                sqs_config.get_stage_dlq_name))
            return

        logger.info('Received {} messages'.format(len(messages)))
        for message in messages:
            logger.info('Starting State Machine Execution')
            if isinstance(message.body, str):
                response = json.loads(message.body)
            StatesInterface().run_state_machine(
                state_config.get_stage_state_machine_arn, response)
            message.delete()
            logger.info('Delete message succeeded')
    except Exception as e:
        logger.error("Fatal error", exc_info=True)
        raise e
    return