del /s/q dist\*
del /s/q build\*
python setup.py sdist
python setup.py bdist_wheel --universal

twine upload dist/*