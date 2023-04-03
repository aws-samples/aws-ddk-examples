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

from importlib import import_module

from datalake_library.commons import init_logger
from datalake_library.configuration.resource_configs import DynamoConfiguration
from datalake_library.interfaces.dynamo_interface import DynamoInterface

logger = init_logger(__name__)


class TransformHandler:
    def __init__(self):
        logger.info("Transformation Handler initiated")

    def stage_transform(self, team, dataset, stage):
        """Returns relevant stage Transformation

        Arguments: 
            team {string} -- Team owning the transformation 
            dataset {string} -- Dataset targeted by transformation 
        Returns: 
            class -- Transform object 
        """
        stage_suffix = stage[-1].lower()
        dynamo_config = DynamoConfiguration()
        dynamo_interface = DynamoInterface(dynamo_config)
        dataset_transforms = dynamo_interface.get_transform_table_item(
            '{}-{}'.format(team, dataset))['transforms']['stage_{}_transform'.format(stage_suffix)]
        transform_info = "datalake_library.transforms.stage_{}_transforms.{}".format(
            stage_suffix, dataset_transforms)
        return getattr(import_module(transform_info), 'CustomTransform')
