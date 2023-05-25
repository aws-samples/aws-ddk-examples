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
from datalake_library.configuration.resource_configs import StateMachineConfiguration
from datalake_library.interfaces.states_interface import StatesInterface

logger = init_logger(__name__)


def lambda_handler(event, context):
    try:
        logger.info('Received {} messages'.format(len(event['Records'])))
        for record in event['Records']:
            logger.info('Starting State Machine Execution')
            event_body = json.loads(record['body'])
            state_config = StateMachineConfiguration(event_body['team'],
                                                     event_body['pipeline'],
                                                     event_body['pipeline_stage'])
            StatesInterface().run_state_machine(
                state_config.get_stage_state_machine_arn, record['body'])
    except Exception as e:
        logger.error("Fatal error", exc_info=True)
        raise e
    return