from setuptools import setup

'''
How to build and upload this package to PyPi
    python setup.py sdist
    python setup.py bdist_wheel --universal
    twine upload dist/*
'''

setup(
    name='robotslacker-sqlcli',
    version='0.0.1',
    description='SQL Command tool, use JDBC, jaydebeapi',
    keywords='sql command jaydebeapi',
    platforms='any',
    install_requires=['jaydebeapi', 'click', ],

    author='RobotSlacker',
    author_email='184902652@qq.com',
    url='https://github.com/robotslacker/robotframework-comparelibrary',

    # packages=['sqlcli'],
    # package_data={'CompareLibrary': ['tests/*.txt']}
)
