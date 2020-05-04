del /s/q dist\*
del /s/q build\*
python setup.py sdist
python setup.py bdist_wheel --universal
pip uninstall robotslacker_sqlcli
python setup.py install
twine upload dist/*