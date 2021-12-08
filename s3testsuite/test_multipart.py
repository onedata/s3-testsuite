"""Authors: Bartek Kryza
Copyright (C) 2021 Onedata.org
This software is released under the MIT license cited in 'LICENSE.txt'
"""

import pprint
import hashlib
import random

import pytest
from .common import s3_client, uuid_str, bucket


def random_range(r):
    res = list(range(r))
    random.shuffle(res)
    return res

@pytest.mark.parametrize(
    "parts,order",
    [
        pytest.param(
            list([b'1' * 1_000]), range(1)
        ),
        pytest.param(
            list([b'1' * 6_000_000, b'2' * 6_000_000, b'3' * 1_000_000]), range(3)
        ),
        pytest.param(
            [b'1' * 6_000_000, b'2' * 6_000_000, b'3' * 1_000_000], reversed(list(range(3)))
        ),
        pytest.param(
            list([b'1' * (6_000_000+i*1024) for i in range(5)]), random_range(5)
        ),
    ],
)
def test_multipart_upload(s3_client, bucket, uuid_str, parts, order):
    key = uuid_str

    parts_md5 = [hashlib.md5(p).digest() for p in parts]
    parts_etags = [hashlib.md5(p).hexdigest() for p in parts]

    pprint.pprint(parts_etags)

    multipart_md5 = b''.join(parts_md5)

    multipart_etag = hashlib.md5(multipart_md5).hexdigest() + "-" + str(len(parts))

    res = s3_client.create_multipart_upload(Bucket=bucket, Key=key)

    assert (res['Key'] == key)
    upload_id = res['UploadId']

    for i in order:
        part_number = i + 1
        res = s3_client.upload_part(Bucket=bucket, Key=key, Body=parts[i], PartNumber=part_number, UploadId=upload_id)
        assert (res['ETag'] == '"' + parts_etags[i] + '"')

    res = s3_client.list_parts(Bucket=bucket, Key=key, UploadId=upload_id)

    assert (len(res['Parts']) == len(parts))
    for p in res['Parts']:
        assert (p['ETag'] == '"' + parts_etags[p["PartNumber"] - 1] + '"')

    res = s3_client.complete_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id,
                                              MultipartUpload={
                                                  'Parts': [{'ETag': p['ETag'], 'PartNumber': p['PartNumber']} for p in
                                                            res['Parts']]})

    assert (res['Key'] == key)

    assert (res['ETag'] == '"' + multipart_etag + '"')
