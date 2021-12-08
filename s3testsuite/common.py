"""Authors: Bartek Kryza
Copyright (C) 2021 Onedata.org
This software is released under the MIT license cited in 'LICENSE.txt'
"""
import os
import random
import string

import pytest
from botocore.config import Config
import boto3
import uuid

#
# Uncomment to trace boto3 requests
#
# import logging
# boto3.set_stream_logger('', logging.DEBUG)


def random_int(lower_bound=1, upper_bound=100):
    return random.randint(lower_bound, upper_bound)


def random_str(size=random_int(),
               characters=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(characters) for _ in range(size))


def random_bytes(size=random_int()):
    return random_str(size).encode('utf-8')


def clean_bucket(s3_client, name, prefix=''):
    continuation_token = ''
    while True:
        res = s3_client.list_objects_v2(Bucket=name, Delimiter='/', EncodingType='url', MaxKeys=1000, Prefix=prefix,
                                        ContinuationToken=continuation_token)
        if 'CommonPrefixes' in res:
            for cp in res['CommonPrefixes']:
                p = cp['Prefix']
                clean_bucket(s3_client, name, p)

        if 'Contents' in res:
            for k in res['Contents']:
                key = k['Key']
                print(f'Removing object {key}')
                s3_client.delete_object(Bucket=name, Key=key)

        if res['IsTruncated']:
            continuation_token = res['NextContinuationToken']
        else:
            break


@pytest.fixture
def uuid_str():
    return str(uuid.uuid4())


def s3_client_create():
    s3_config = Config(
        region_name='us-east-1', signature_version='s3v4',
        retries={
            'max_attempts': 1,
            'mode': 'standard'
        }
    )

    return boto3.client(
        service_name='s3',
        endpoint_url=os.getenv('AWS_S3_ENDPOINT', 'http://0.0.0.0:8080'),
        verify=False,
        config=s3_config
    )

@pytest.fixture
def s3_client():
    return s3_client_create()


@pytest.fixture
def bucket(s3_client, uuid_str):
    s3_client.create_bucket(Bucket=uuid_str)
    yield uuid_str
    clean_bucket(s3_client, uuid_str)
    s3_client.delete_bucket(Bucket=uuid_str)



