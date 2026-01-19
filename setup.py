# -*- coding: utf8 -*-
# This is purely the result of trial and error.

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
        doc_match = re.search(r'"""(.*?)"""', content, re.DOTALL)

        version = version_match.group(1) if version_match else '1.0.15'
        author = author_match.group(1) if author_match else 'Ivan Begtin'
        doc = doc_match.group(1).strip() if doc_match else 'undatum: a command-line tool for data processing'

        return version, author, doc


def long_description():
    with codecs.open('README.md', encoding='utf8') as f:
        return f.read()


# Read version and metadata
__version__, __author__, __doc__ = read_version()

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
    entry_points={
        'console_scripts': [
            'undatum = undatum.__main__:main',
            'data = undatum.__main__:main',
        ],
    },
    python_requires='>=3.9',
    zip_safe=False,
    keywords='json jsonl csv bson cli dataset',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.9',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'Topic :: Software Development',
        'Topic :: System :: Networking',
        'Topic :: Terminals',
        'Topic :: Text Processing',
        'Topic :: Utilities'
    ],
)
