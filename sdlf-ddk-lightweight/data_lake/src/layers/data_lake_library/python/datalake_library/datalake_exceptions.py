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

class ObjectDeleteFailedException(Exception):
    """Raised when the lambda fails to delete a file(s)"""
    pass


class InvalidS3PutEventException(Exception):
    """Raised when the object added to the bucket according to the provided event does not match the expected pattern"""
    pass


class UnprocessedKeysException(RuntimeError):
    """Raised when keys are unprocessed, either because the batch limit is exceeded, the size of the response is too big
   (>16Mb) or the keys were throttled because of ProvisionedReads too low on ddb"""
    pass
