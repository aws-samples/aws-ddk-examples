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

import logging

logger = logging.getLogger(__name__)


def clean_table(dynamodb, table_name, pk_name, sk_name=""):
    logger.debug(f"Clean dynamodb table {table_name}, PK: {pk_name}, SK: {sk_name}")
    table = dynamodb.Table(table_name)
    while True:
        result = table.scan()
        if result["Count"] == 0:
            logger.debug(f"Clean dynamodb table {table_name}... DONE")
            return

        with table.batch_writer() as batch:
            for item in result["Items"]:
                if not sk_name:
                    batch.delete_item(Key={pk_name: item[pk_name]})
                else:
                    batch.delete_item(Key={pk_name: item[pk_name], sk_name: item[sk_name]})
