del /s/q dist\*
del /s/q build\*
python setup.py sdist
python setup.py bdist_wheel --universal
pip uninstall --yes robotslacker_sqlcli
python setup.py install
REM twine upload dist/*