# -*- coding: utf-8 -*-

import ast
import re
from io import open
from setuptools import setup

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
    with open(filename, 'r+', encoding='utf-8') as rf:
        return rf.read()


readme = open_file("README.md")

setup(
    name='robotslacker-sqlcli-noodbc',
    version=version,
    description='SQL Command test tool, use JDBC',
    long_description=readme,
    keywords='sql command test tool',
    long_description_content_type='text/markdown',
    platforms='any',
    install_requires=['JPype1', 'setproctitle', 'pathlib', 'urllib3',
                      'pyparsing', 'click', 'prompt_toolkit',
                      'fs', 'hdfs', 'wget', 'httptools', 'pika', 'paramiko',
                      'numpy'
                      ],

    author='RobotSlacker',
    author_email='184902652@qq.com',
    url='https://github.com/robotslacker/sqlcli',

    zip_safe=False,
    packages=['sqlcli'],
    package_data={'sqlcli': ['jlib/README', 'conf/*ini', 'profile/*']},
    python_requires='>=3.6',
    entry_points={
        "console_scripts": ["sqlcli = sqlcli.main:cli"],
        "distutils.commands": ["lint = tasks:lint", "test = tasks:test"],
    },
)
