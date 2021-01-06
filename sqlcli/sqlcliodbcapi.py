# -*- coding: utf-8 -*-

'''
http://www.python.org/topics/database/DatabaseAPI-2.0.html
'''

import ceODBC


# DB-API 2.0 Module Interface connect constructor
def connect(p_szODBCURL):
    print("URL=[" + p_szODBCURL + "]")
    try:
        conn = ceODBC.connect(p_szODBCURL)
    except Exception as e:
        print("E: " + str(repr(e)))
