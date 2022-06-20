# -*- coding: utf-8 -*-


class SQLCliException(Exception):
    def __init__(self, message):
        Exception.__init__(self)
        self.message = message

    def __str__(self):
        return self.message
