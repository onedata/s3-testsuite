# s3-testsuite

Suite of basic pytest test cases for s3 compatible services.

## Running

```bash
git clone https://github.com/onedata/s3-testsuite
cd s3-testsuite

# Optionally setup venv
virtualenv -p /usr/bin/python3 venv
. venv/bin/activate

# Install requirements
pip3 install -r requirements.txt

# Export S3 service details
export AWS_S3_ENDPOINT=https://127.0.0.1:8081
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...

# run all tests
python -m pytest --verbose s3testsuite

# or run specific tests using
python -m pytest --verbose -k test_multipart_upload,test_get_object_range s3testsuite
```