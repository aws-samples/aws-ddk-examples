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


import boto3
import json
import sys


profile_name = str(sys.argv[1])
session = boto3.session.Session(profile_name=profile_name)

s3_client = session.client('s3')
s3_resource = session.resource('s3')

if __name__ == "__main__":
    try: 
        with open('delete_file.json') as json_data:
            items = json.load(json_data)
            for bucket_name in items["s3"]:
                print(f"Emptying Content From: {bucket_name}")
                response = s3_client.list_objects_v2(Bucket=bucket_name)
                versions = s3_client.list_object_versions(Bucket=bucket_name) # list all versions in this bucket
                if 'Contents' in response:
                    for item in response['Contents']:
                        print('deleting file', item['Key'])
                        s3_client.delete_object(Bucket=bucket_name, Key=item['Key'])
                        while response['KeyCount'] == 1000:
                            response = s3_client.list_objects_v2(
                            Bucket=bucket_name,
                            StartAfter=response['Contents'][0]['Key'],
                            )
                            for item in response['Contents']:
                                print('deleting file', item['Key'])
                                s3_client.delete_object(Bucket=bucket_name, Key=item['Key'])
                if 'Versions' in versions and len(versions['Versions'])>0:
                    s3_bucket = s3_resource.Bucket(bucket_name)
                    s3_bucket.object_versions.delete()
                print(f"Bucket: {bucket_name} is Empty")
            
            json_data.close()

    except Exception as e:
        print(f"Error: {e}")


