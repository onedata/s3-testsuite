"""Authors: Bartek Kryza
Copyright (C) 2021 Onedata.org
This software is released under the MIT license cited in 'LICENSE.txt'
"""

import os
import re

from setuptools import find_packages, setup


def get_version():
    """Return package version as listed in `__version__` in `init.py`."""
    init_py = open('s3-testsuite/__init__.py').read()
    return re.search("__version__ = ['\"]([^'\"]+)['\"]", init_py).group(1)


version = get_version()


def read(fname):
    """Read description from local file."""
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


requirements = []

setup(
    name="s3-testsuite",
    version=version,
    description="`s3-testsuite` set of basic tests for s3 services.",
    long_description=read('README.md'),
    url='https://github.com/onedata/s3-testsuite',
    license='MIT',
    author='Bartek Kryza',
    author_email='bkryza@gmail.com',
    packages=find_packages(),
    include_package_data=True,
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10'
    ],
    install_requires=['pytest', 'boto3']
)