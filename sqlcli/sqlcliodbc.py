# -*- coding: utf-8 -*-

def connect(p_connecturl):
    if p_connecturl:
        pass
    raise SQLCliODBCException("SQLCLI-0000: Unable to find the local dynamic library. \n"
                              "You may need to reinstall or use 'Python setup.py build_ext' to generate it.")


class SQLCliODBCException(Exception):
    def __init__(self, message):
        Exception.__init__(self)
        self.message = message

    def __str__(self):
        return self.message
