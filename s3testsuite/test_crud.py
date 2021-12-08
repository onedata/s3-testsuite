"""Authors: Bartek Kryza
Copyright (C) 2021 Onedata.org
This software is released under the MIT license cited in 'LICENSE.txt'
"""

import hashlib

import pytest
from .common import s3_client, s3_client_create, uuid_str, bucket, random_bytes


def test_put_object(s3_client, bucket, uuid_str):
    key = uuid_str

    body = random_bytes()
    etag = hashlib.md5(body).hexdigest()

    s3_client.put_object(Bucket=bucket, Key=key, Body=body)
    res = s3_client.get_object(Bucket=bucket, Key=key)

    assert(res['ContentLength'] == len(body))
    assert(res['ETag'] == f'"{etag}"')


@pytest.mark.parametrize(
    "prefix",
    [
        pytest.param('dir1/'), pytest.param('dir1/dir1/dir1/dir1/')
    ],
)
def test_put_object_with_prefix(s3_client, bucket, uuid_str,  prefix):
    key = prefix + uuid_str

    body = random_bytes()
    etag = hashlib.md5(body).hexdigest()

    s3_client.put_object(Bucket=bucket, Key=key, Body=body)
    res = s3_client.get_object(Bucket=bucket, Key=key)

    assert(res['ContentLength'] == len(body))
    assert(res['ETag'] == f'"{etag}"')


@pytest.mark.parametrize(
    "prefix_list,count",
    [
        pytest.param(['dir1/', 'dir1/dir2/', 'dir1/dir2/', 'dir2/dir3/dir4/dir5/'], 10),
        pytest.param(['dir50/'], 50)
    ],
)
def test_put_and_get_objects(s3_client, bucket, prefix_list, count):
    keys = {}

    for prefix in prefix_list:
        for i in range(count):
            body = random_bytes()
            etag = hashlib.md5(body).hexdigest()
            key = f'{prefix}file-{i}.txt'

            s3_client.put_object(Bucket=bucket, Key=key, Body=body)

            keys[key] = {'body': body, 'etag': etag}

    for key in keys.keys():
        res = s3_client.get_object(Bucket=bucket, Key=key)
        assert (res['ContentLength'] == len(keys[key]['body']))
        assert (res['ETag'] == f'"{keys[key]["etag"]}"')


@pytest.mark.parametrize(
    "prefix_list,count",
    [
        pytest.param(['dir1/', 'dir1/dir2/', 'dir1/dir2/', 'dir2/dir3/dir4/dir5/'], 25),
        pytest.param(['dir50/'], 100)
    ],
)
def test_put_objects_large(bucket, s3_client, prefix_list, count, thread_count=100):
    from concurrent.futures import ThreadPoolExecutor

    def put_object(job):
        bucket_, key_, body_, etag_ = job
        res = s3_client.put_object(Bucket=bucket_, Key=key_, Body=body_)
        assert (res['ContentLength'] == len(body_))
        assert (res['ETag'] == f'"{etag_}"')

    keys = []

    for prefix in prefix_list:
        for i in range(count):
            body = random_bytes()
            etag = hashlib.md5(body).hexdigest()
            key = f'{prefix}file-{i}.txt'
            keys.append((bucket, key, body, etag))

    with ThreadPoolExecutor(thread_count) as pool:
        pool.map(put_object, keys)


def test_delete_object(s3_client, bucket, uuid_str):
    key = uuid_str

    body = random_bytes()

    s3_client.put_object(Bucket=bucket, Key=key, Body=body)

    s3_client.get_object(Bucket=bucket, Key=key)

    s3_client.delete_object(Bucket=bucket, Key=key)

    with pytest.raises(s3_client.exceptions.NoSuchKey) as excinfo:
        s3_client.get_object(Bucket=bucket, Key=key)

    assert 'The specified key does not exist' in str(excinfo.value)


def test_delete_objects(s3_client, bucket):
    body = random_bytes()

    files = [f'f-{i}.txt' for i in range(10)]

    for f in files:
        s3_client.put_object(Bucket=bucket, Key=f, Body=body)

    res = s3_client.list_objects_v2(Bucket=bucket, Delimiter='/', EncodingType='path', MaxKeys=1000, Prefix='')

    assert(res['KeyCount'] == 10)

    s3_client.delete_objects(Bucket=bucket, Delete={'Objects': [{'Key': f} for f in files]})

    res = s3_client.list_objects_v2(Bucket=bucket, Delimiter='/', EncodingType='path', MaxKeys=1000, Prefix='')

    assert(res['KeyCount'] == 0)


def test_get_object_range(s3_client, bucket, uuid_str):
    key = uuid_str

    body = random_bytes()
    etag = hashlib.md5(body).hexdigest()

    s3_client.put_object(Bucket=bucket, Key=key, Body=body)
    res = s3_client.get_object(Bucket=bucket, Key=key, Range="bytes=2-4")

    assert(res['ContentLength'] == 3)
    assert(res['ETag'] == f'"{etag}"')
    assert(res['Body'].read() == body[2:5])


def test_head_object(s3_client, bucket, uuid_str):
    key = uuid_str

    body = random_bytes()
    etag = hashlib.md5(body).hexdigest()

    s3_client.put_object(Bucket=bucket, Key=key, Body=body)
    res = s3_client.head_object(Bucket=bucket, Key=key)

    assert(res['ContentLength'] == len(body))
    assert(res['ETag'] == f'"{etag}"')


@pytest.mark.skip
def test_copy_object(s3_client, bucket, uuid_str):
    key = uuid_str

    body = random_bytes()
    etag = hashlib.md5(body).hexdigest()

    s3_client.put_object(Bucket=bucket, Key=key, Body=body)
    res = s3_client.copy_object(Bucket=bucket, CopySource=key, Key=f'{key}-copy')

    res = s3_client.get_object(Bucket=bucket, Key=f'{key}-copy')

    assert(res['ContentLength'] == len(body))
    assert(res['ETag'] == f'"{etag}"')
