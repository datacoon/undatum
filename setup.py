# -*- coding: utf8 -*-
# This is purely the result of trial and error.

import sys
import codecs

from setuptools import setup, find_packages
from setuptools.command.test import test as TestCommand

import undatum


class PyTest(TestCommand):
    # `$ python setup.py test' simply installs minimal requirements
    # and runs the tests with no fancy stuff like parallel execution.
    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = [
            '--doctest-modules', '--verbose',
            './undatum', './tests'
        ]
        self.test_suite = True

    def run_tests(self):
        import pytest
        sys.exit(pytest.main(self.test_args))


tests_require = [
    # Pytest needs to come last.
    # https://bitbucket.org/pypa/setuptools/issue/196/
    'pytest',
    'mock',
]


install_requires = [
    'chardet>=3.0.4',
    'click>=8.0.3',
    'dictquery>=0.4.0',
    'jsonlines>=1.2.0',
    'openpyxl>=3.0.5',
    'orjson>=3.6.6',
    'pandas>=1.1.3',
    'pymongo>=3.11.0',
    'qddate>=0.1.1',
    'tabulate>=0.8.7',
    'validators>=0.18.1',
    'xlrd>=1.2.0',
    'xmltodict',
    'rich',
    'duckdb',
    'pyzstd',
    'pydantic'
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
    with codecs.open('README.rst', encoding='utf8') as f:
        return f.read()


setup(
    name='undatum',
    version=undatum.__version__,
    description=undatum.__doc__.strip(),
    long_description=long_description(),
    long_description_content_type='text/x-rst',
    url='https://github.com/datacoon/undatum/',
    download_url='https://github.com/datacoon/undatum/',
    packages=find_packages(exclude=('tests', 'tests.*')),
    include_package_data=True,
    author=undatum.__author__,
    author_email='ivan@begtin.tech',
    license=undatum.__licence__,
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
    cmdclass={'test': PyTest},
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
