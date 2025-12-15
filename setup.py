# -*- coding: utf8 -*-
# This is purely the result of trial and error.

import sys
import codecs
import re
import os

from setuptools import setup, find_packages


def read_version():
    """Read version from __init__.py without importing the module."""
    init_path = os.path.join(os.path.dirname(__file__), 'undatum', '__init__.py')
    with open(init_path, 'r', encoding='utf-8') as f:
        content = f.read()
        version_match = re.search(r"__version__\s*=\s*['\"]([^'\"]+)['\"]", content)
        author_match = re.search(r"__author__\s*=\s*['\"]([^'\"]+)['\"]", content)
        licence_match = re.search(r"__licence__\s*=\s*['\"]([^'\"]+)['\"]", content)
        doc_match = re.search(r'"""(.*?)"""', content, re.DOTALL)
        
        version = version_match.group(1) if version_match else '1.0.15'
        author = author_match.group(1) if author_match else 'Ivan Begtin'
        licence = licence_match.group(1) if licence_match else 'MIT'
        doc = doc_match.group(1).strip() if doc_match else 'undatum: a command-line tool for data processing'
        
        return version, author, licence, doc


tests_require = [
    # Pytest needs to come last.
    # https://bitbucket.org/pypa/setuptools/issue/196/
    'pytest',
    'mock',
]


install_requires = [
    'avro>=1.10.2',
    'chardet>=5.2.0',
    'click>=8.0.3',
    'dictquery>=0.5.0',
    'duckdb',
    'elasticsearch',
    'iterabledata',
    'jsonlines>=4.0.0',
    'lz4>=4.3.2',
    'mistql>=0.4.11',
    'openpyxl>=3.1.2',
    'orjson>=3.9.8',
    'pandas>=2.0.3',
    'py7zr>=0.20.6',
    'pydantic',
    'pymongo>=4.5.0',
    'pyorc>=0.8.0',
    'python-docx>=0.8.11',
    'pyyaml',
    'pyzstd',
    'qddate>=1.0.4',
    'requests',
    'rich>=13.6.0',
    'tabulate>=0.8.7',
    'tqdm',
    'typer',
    'validators>=0.22.0',
    'xlrd>=2.0.1',
    'xlwt',
    'xmltodict>=0.13.0',
    'xxhash',
]


# Conditional dependencies:

# sdist
if 'bdist_wheel' not in sys.argv:
    try:
        # noinspection PyUnresolvedReferences
        import argparse
    except ImportError:
        install_requires.append('argparse>=1.2.1')


# bdist_wheel
extras_require = {
    # https://wheel.readthedocs.io/en/latest/#defining-conditional-dependencies
    'python_version == "3.8" or python_version == "3.8"': ['argparse>=1.2.1'],
}


def long_description():
    with codecs.open('README.md', encoding='utf8') as f:
        return f.read()


# Read version and metadata
__version__, __author__, __licence__, __doc__ = read_version()

setup(
    name='undatum',
    version=__version__,
    description=__doc__,
    long_description=long_description(),
    long_description_content_type='text/markdown',
    url='https://github.com/datacoon/undatum/',
    download_url='https://github.com/datacoon/undatum/',
    packages=find_packages(exclude=('tests', 'tests.*')),
    include_package_data=True,
    author=__author__,
    author_email='ivan@begtin.tech',
    license=__licence__,
    entry_points={
        'console_scripts': [
            'undatum = undatum.__main__:main',
            'data = undatum.__main__:main',
        ],
    },
    extras_require=extras_require,
    install_requires=install_requires,
    tests_require=tests_require,
    python_requires='>=3.8',
    zip_safe=False,
    keywords='json jsonl csv bson cli dataset',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.8',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: BSD License',
        'Topic :: Software Development',
        'Topic :: System :: Networking',
        'Topic :: Terminals',
        'Topic :: Text Processing',
        'Topic :: Utilities'
    ],
)
