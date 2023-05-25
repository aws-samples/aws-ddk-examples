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
from dataclasses import dataclass
from typing import Any, Optional

import aws_cdk as cdk
from aws_cdk.custom_resources import Provider
from aws_ddk_core import BaseStack
from constructs import Construct


@dataclass
class CustomDatasetConfig:
    team: str
    dataset: str
    pipeline: str
    stage_a_transform: str
    register_provider: Provider


class CustomDatasetStack(BaseStack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        environment_id: str,
        resource_prefix: str,
        config: CustomDatasetConfig,
        **kwargs: Any,
    ) -> None:
        self._dataset_config: CustomDatasetConfig = config
        self._team = self._dataset_config.team
        self._pipeline = self._dataset_config.pipeline
        self._dataset = self._dataset_config.dataset
        self._resource_prefix = resource_prefix
        super().__init__(
            scope,
            construct_id,
            environment_id=environment_id,
            stack_name=f"{self._resource_prefix}-CustomDataset-{self._team}"
            + f"-{self._dataset}-{environment_id}",
            **kwargs,
        )

        self._stage_a_transform = self._dataset_config.stage_a_transform

        self._register_octagon_configs(
            self._team, self._pipeline, self._dataset, self._stage_a_transform
        )

    def _register_octagon_configs(
        self,
        team: str,
        pipeline: str,
        dataset_name: str,
        stage_a_transform: Optional[str] = None,
    ):
        self.stage_a_transform: str = (
            stage_a_transform if stage_a_transform else "light_transform_blueprint"
        )

        self._props = {
            "id": f"{team}-{dataset_name}",
            "description": f"{dataset_name.title()} dataset",
            "name": f"{team}-{dataset_name}",
            "type": "octagon_dataset",
            "pipeline": pipeline,
            "version": 1,
            "transforms": {"stage_a_transform": self.stage_a_transform},
        }

        service_setup_properties = {"RegisterProperties": json.dumps(self._props)}

        cdk.CustomResource(
            self,
            f"{self._props['id']}-{self._props['type']}-custom-resource",
            service_token=self._dataset_config.register_provider.service_token,
            properties=service_setup_properties,
        )
