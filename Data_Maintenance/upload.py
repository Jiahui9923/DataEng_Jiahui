#!/usr/bin/env python

# Copyright 2019 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys

# [START storage_upload_file]
from google.cloud import storage
import zlib
import json

from numpy import byte
from pytest import Item
from yaml import load


bucket_name = "activity0223"
source_file_name = "May-20-2022.json"
destination_blob_name = "test_data_file"

def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"
    # The path to your file to upload
    # source_file_name = "local/path/to/file"
    # The ID of your GCS object
    # destination_blob_name = "storage-object-name"
    with open ("May-20-2022.json",'r') as load_f:
        load_dict = json.load(load_f)

    print(type(load_dict))
    string1 = ''.join([str(item) for item in load_dict])

    bytes_data = bytes(string1,'utf-8')
    compressed_data = zlib.compress(bytes_data, 1)

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_string(compressed_data)

    print(
        f"String uploaded to {destination_blob_name}."
    )


# [END storage_upload_file]

if __name__ == "__main__":
    upload_blob(
        bucket_name,
        source_file_name,
        destination_blob_name,
    )