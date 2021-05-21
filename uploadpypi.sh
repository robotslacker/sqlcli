PYTHON_HOME=/opt/anaconda3
PYTHON_BIN=$PYTHON_HOME/bin/python
PIP_BIN=$PYTHON_HOME/bin/pip

rm -f -r dist
rm -f -r build

$PYTHON_BIN setup.py build_ext
$PYTHON_BIN setup.py sdist
$PYTHON_BIN setup.py bdist_wheel --universal
$PIP_BIN uninstall --yes robotslacker_sqlcli
$PYTHON_BIN setup.py install
echo "twine upload dist/*tar.gz"
