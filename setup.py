# This is purely the result of trial and error.

import sys
import codecs

from setuptools import setup, find_packages
from setuptools.command.test import test as TestCommand

import datum


class PyTest(TestCommand):
    # `$ python setup.py test' simply installs minimal requirements
    # and runs the tests with no fancy stuff like parallel execution.
    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = [
            '--doctest-modules', '--verbose',
            './datum', './tests'
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
    'bson',
    'click',
    'xlrd',
    'openpyxl',
    'validators',
    'qddate',
    'tabulate',
    'dictquery'
]


# Conditional dependencies:

# sdist
if 'bdist_wheel' not in sys.argv:
    try:
        # noinspection PyUnresolvedReferences
        import argparse
    except ImportError:
        install_requires.append('argparse>=1.2.1')

    if 'win32' in str(sys.platform).lower():
        # Terminal colors for Windows
        install_requires.append('colorama>=0.2.4')


# bdist_wheel
extras_require = {
    # https://wheel.readthedocs.io/en/latest/#defining-conditional-dependencies
    'python_version == "3.0" or python_version == "3.1"': ['argparse>=1.2.1'],
    ':sys_platform == "win32"': ['colorama>=0.2.4'],
}


def long_description():
    with codecs.open('README.rst', encoding='utf8') as f:
        return f.read()


setup(
    name='datum',
    version=datum.__version__,
    description=datum.__doc__.strip(),
    long_description=long_description(),
    url='https://github.com/infoculture/datum/',
    download_url='https://github.com/infoculture/datum/',
    author=datum.__author__,
    author_email='ivan@begtin.tech',
    license=datum.__licence__,
    packages=find_packages(),
    entry_points={
        'console_scripts': [
            'datum = datum.__main__:main',
            'data = datum.__main__:main',
        ],
    },
    extras_require=extras_require,
    install_requires=install_requires,
    tests_require=tests_require,
    cmdclass={'test': PyTest},
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
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
