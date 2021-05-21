del /s/q dist\*
del /s/q build\*
del sqlcli\sqlcliodbc*.pyd
python setup.py build_ext
python setup.py sdist
python setup.py bdist_wheel --universal
pip uninstall --yes robotslacker_sqlcli
python setup.py install
REM pylint --rcfile pylint.conf --disable C0103 sqlcli
REM twine upload dist/*