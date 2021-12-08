"""Authors: Bartek Kryza
Copyright (C) 2021 Onedata.org
This software is released under the MIT license cited in 'LICENSE.txt'
"""

import pprint
import unittest
import pytest
from .common import s3_client, uuid_str, bucket


def test_create_delete_bucket(s3_client, uuid_str):
    name = uuid_str

    s3_client.create_bucket(Bucket=name)
    res = s3_client.list_buckets()
    buckets = res['Buckets']

    assert(list(map(lambda b: b['Name'] == name, buckets)).count(True) == 1)

    s3_client.delete_bucket(Bucket=name)

    res = s3_client.list_buckets()
    buckets = res['Buckets']

    assert(list(map(lambda b: b['Name'] == name, buckets)).count(True) == 0)


def test_create_delete_nonempty_bucket(s3_client, uuid_str):
    name = uuid_str

    s3_client.create_bucket(Bucket=name)
    res = s3_client.list_buckets()
    buckets = res['Buckets']

    assert(list(map(lambda b: b['Name'] == name, buckets)).count(True) == 1)

    s3_client.put_object(Bucket=name, Key='file.txt', Body=b'TEST')

    with pytest.raises(Exception, match="The bucket you tried to delete is not empty") as excinfo:
        s3_client.delete_bucket(Bucket=name)

    assert 'The bucket you tried to delete is not empty' in str(excinfo.value)

    s3_client.delete_object(Bucket=name, Key='file.txt')

    res = s3_client.list_objects_v2(Bucket=name, Delimiter='/', EncodingType='url', MaxKeys=1000, Prefix='')

    pprint.pprint(res)

    assert(res['KeyCount'] == 0)

    s3_client.delete_bucket(Bucket=name)


    res = s3_client.list_buckets()
    buckets = res['Buckets']

    assert(list(map(lambda b: b['Name'] == name, buckets)).count(True) == 0)


@pytest.mark.parametrize(
    "encoding_type",
    [
        pytest.param('path'), pytest.param('url')
    ],
)
def test_list_empty_bucket(s3_client, bucket, encoding_type):
    res = s3_client.list_objects(Bucket=bucket, Delimiter='/', EncodingType=encoding_type, MaxKeys=1000, Prefix='')

    assert('Contents' not in res)
    assert(res['Name'] == bucket)
    assert (res['Name'] == bucket)


@pytest.mark.parametrize(
    "encoding_type",
    [
        pytest.param('path'), pytest.param('url')
    ],
)
def test_list_v2_empty_bucket(s3_client, bucket, encoding_type):
    res = s3_client.list_objects_v2(Bucket=bucket, Delimiter='/', EncodingType=encoding_type, MaxKeys=1000, Prefix='')

    assert(res['KeyCount'] == 0)
    assert(res['Name'] == bucket)
    assert (res['Name'] == bucket)


@pytest.mark.parametrize(
    "encoding_type",
    [
        pytest.param('path'), pytest.param('url')
    ],
)
def test_list_small_bucket(s3_client, bucket, encoding_type):
    for i in range(20):
        s3_client.put_object(Bucket=bucket, Key=f'file-{i}.txt', Body=b'TEST')

    res = s3_client.list_objects(Bucket=bucket, Delimiter='/', EncodingType=encoding_type, MaxKeys=1000, Prefix='')

    assert(len(res['Contents']) == 20)
    assert(res['Name'] == bucket)
    assert (res['Name'] == bucket)


@pytest.mark.parametrize(
    "encoding_type",
    [
        pytest.param('path'), pytest.param('url')
    ],
)
def test_list_v2_small_bucket(s3_client, bucket, encoding_type):
    for i in range(20):
        s3_client.put_object(Bucket=bucket, Key=f'file-{i}.txt', Body=b'TEST')

    res = s3_client.list_objects_v2(Bucket=bucket, Delimiter='/', EncodingType=encoding_type, MaxKeys=1000, Prefix='')

    assert(res['KeyCount'] == 20)
    assert(res['Name'] == bucket)
    assert (res['Name'] == bucket)


if __name__ == '__main__':
    unittest.main()
