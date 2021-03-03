# -*- coding: utf-8 -*-

import ast
import re
import os
import sys
from io import open
from setuptools import setup, Extension

'''
How to build and upload this package to PyPi
    python setup.py sdist
    python setup.py bdist_wheel --universal
    twine upload dist/*

How to build and upload this package to Local site:
    python setup.py install
'''

_version_re = re.compile(r"__version__\s+=\s+(.*)")

if sys.platform == "win32":
    libs = ["odbc32"]
else:
    libs = ["odbc"]

# define ODBC sources
sourceDir = "sqlcli/odbc"
sources = [os.path.join(sourceDir, n) for n in sorted(os.listdir(sourceDir)) if n.endswith(".c")]
depends = [os.path.join(sourceDir, "ceoModule.h")]

# setup the extension
extension = Extension(
        name="SQLCliODBC",
        libraries=libs,
        define_macros=[("BUILD_VERSION", "3.0")],
        sources=sources,
        depends=depends)

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
    name='robotslacker-sqlcli',
    version=version,
    description='SQL Command test tool, use JDBC/ODBC',
    long_description=readme,
    keywords='sql command test tool',
    long_description_content_type='text/markdown',
    platforms='any',
    install_requires=['JPype1>=0.7.1', 'setproctitle', 'pathlib',
                      'pyparsing', 'click', 'prompt_toolkit',
                      'cli_helpers', 'fs', 'hdfs', 'wget'],

    author='RobotSlacker',
    author_email='184902652@qq.com',
    url='https://github.com/robotslacker/sqlcli',

    zip_safe=False,
    packages=['sqlcli'],
    package_data={'sqlcli': ['jlib/README', 'conf/*ini', 'odbc/*', 'profile/*']},
    python_requires='>=3.6',
    entry_points={
        "console_scripts": ["sqlcli = sqlcli.main:cli"],
        "distutils.commands": ["lint = tasks:lint", "test = tasks:test"],
    },
    ext_modules=[extension],
)
