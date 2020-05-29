# -*- coding: utf-8 -*-

import ast
from io import open
import re
from setuptools import setup, find_packages

'''
How to build and upload this package to PyPi
    python setup.py sdist
    python setup.py bdist_wheel --universal
    twine upload dist/*

How to build and upload this package to Local site:
    python setup.py install
'''

_version_re = re.compile(r"__version__\s+=\s+(.*)")

with open("sqlcli/__init__.py", "rb") as f:
    version = str(
        ast.literal_eval(_version_re.search(f.read().decode("utf-8")).group(1))
    )


def open_file(filename):
    """Open and read the file *filename*."""
    with open(filename, 'r+', encoding='utf-8') as f:
        return f.read()


readme = open_file("README.md")

setup(
    name='robotslacker-sqlcli',
    version=version,
    description='SQL Command tool, use JDBC, jaydebeapi',
    long_description=readme,
    keywords='sql command jaydebeapi',
    platforms='any',
    install_requires=['jaydebeapi', 'setproctitle', 'click', 'prompt_toolkit', 'cli_helpers', 'kafka-python', 'fs'],

    author='RobotSlacker',
    author_email='184902652@qq.com',
    url='https://github.com/robotslacker/sqlcli',

    zip_safe = False,
    packages=['sqlcli'],
    package_data={'sqlcli': ['jlib/*']},

    entry_points={
        "console_scripts": ["sqlcli = sqlcli.main:cli"],
        "distutils.commands": ["lint = tasks:lint", "test = tasks:test"],
    },
)
