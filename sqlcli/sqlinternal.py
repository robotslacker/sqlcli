# -*- coding: utf-8 -*-
import fs
import re
import os
from kafka import KafkaProducer
from .sqlcliexception import SQLCliException


def random_digits(p_arg):
    n = int(p_arg[0])
    seed = '0123456789'.encode()
    len_lc = len(seed)
    ba = bytearray(os.urandom(n))
    for i, b in enumerate(ba):
        ba[i] = seed[b % len_lc]
    return ba.decode('ascii')


def random_ascii_uppercase(p_arg):
    n = int(p_arg[0])
    seed = 'ABCDEFGHIJKLMNOPRRSTUVWXYZ'.encode()
    len_lc = len(seed)
    ba = bytearray(os.urandom(n))
    for i, b in enumerate(ba):
        ba[i] = seed[b % len_lc]
    return ba.decode('ascii')


def random_ascii_lowercase(p_arg):
    n = int(p_arg[0])
    seed = 'abcdefghijklmnopqrstuvwxyz'.encode()
    len_lc = len(seed)
    ba = bytearray(os.urandom(n))
    for i, b in enumerate(ba):
        ba[i] = seed[b % len_lc]
    return ba.decode('ascii')


def random_ascii_letters(p_arg):
    n = int(p_arg[0])
    seed = 'ABCDEFGHIJKLMNOPRRSTUVWXYZabcdefghijklmnopqrstuvwxyz'.encode()
    len_lc = len(seed)
    ba = bytearray(os.urandom(n))
    for i, b in enumerate(ba):
        ba[i] = seed[b % len_lc]
    return ba.decode('ascii')


def random_ascii_letters_and_digits(p_arg):
    n = int(p_arg[0])
    seed = 'ABCDEFGHIJKLMNOPRRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789'.encode()
    len_lc = len(seed)
    ba = bytearray(os.urandom(n))
    for i, b in enumerate(ba):
        ba[i] = seed[b % len_lc]
    return ba.decode('ascii')


def identity(p_arg):
    nStart = int(p_arg[0])
    if not hasattr(identity, 'x'):
        identity.x = nStart
    else:
        identity.x = identity.x + 1
    return str(identity.x)


def parse_formula_str(p_formula_str):
    m_row_struct = re.split('[{}]', p_formula_str.replace('\n', '').replace('\r', ''))

    for m_nRowPos in range(0, len(m_row_struct)):
        if re.search('random_ascii_lowercase|random_ascii_uppercase|random_ascii_letters' +
                     '|random_digits|identity|random_ascii_letters_and_digits',
                     m_row_struct[m_nRowPos], re.IGNORECASE):
            m_function_struct = re.split(r'[(,)]', m_row_struct[m_nRowPos])
            for m_nPos in range(0, len(m_function_struct)):
                m_function_struct[m_nPos] = m_function_struct[m_nPos].strip()
                if m_function_struct[m_nPos].startswith("'"):
                    m_function_struct[m_nPos] = m_function_struct[m_nPos][1:]
                if m_function_struct[m_nPos].endswith("'"):
                    m_function_struct[m_nPos] = m_function_struct[m_nPos][0:-1]
            if len(m_function_struct[-1]) == 0:
                m_function_struct.pop()
            m_call_out_struct = []
            if m_function_struct[0].upper() == "RANDOM_ASCII_LOWERCASE":
                m_call_out_struct.append(random_ascii_lowercase)
                m_call_out_struct.append(m_function_struct[1:])
            if m_function_struct[0].upper() == "RANDOM_ASCII_UPPERCASE":
                m_call_out_struct.append(random_ascii_uppercase)
                m_call_out_struct.append(m_function_struct[1:])
            if m_function_struct[0].upper() == "RANDOM_ASCII_LETTERS":
                m_call_out_struct.append(random_ascii_letters)
                m_call_out_struct.append(m_function_struct[1:])
            if m_function_struct[0].upper() == "RANDOM_DIGITS":
                m_call_out_struct.append(random_digits)
                m_call_out_struct.append(m_function_struct[1:])
            if m_function_struct[0].upper() == "IDENTITY":
                m_call_out_struct.append(identity)
                m_call_out_struct.append(m_function_struct[1:])
            if m_function_struct[0].upper() == "RANDOM_ASCII_LETTERS_AND_DIGITS":
                m_call_out_struct.append(random_ascii_letters_and_digits)
                m_call_out_struct.append(m_function_struct[1:])
            m_row_struct[m_nRowPos] = m_call_out_struct
    return m_row_struct


def get_final_string(p_row_struct):
    m_Result = ""
    for col in p_row_struct:
        if isinstance(col, list):
            m_Result = m_Result + col[0](col[1])
        else:
            m_Result = m_Result + col
    return m_Result


def Create_file(p_filename, p_formula_str, p_rows, p_options):
    try:
        m_producer = None
        m_topicname = None
        m_output = None

        if p_filename.startswith('mem://'):
            m_fs = fs.open_fs('mem://')
            m_filename = p_filename[6:]
            m_output = m_fs.open(m_filename, 'w')
        elif p_filename.startswith('file://'):
            m_fs = fs.open_fs('./')
            m_filename = p_filename[7:]
            m_output = m_fs.open(m_filename, 'w')
        elif p_filename.startswith('tar://'):
            m_fs = fs.open_fs(p_filename + ".tar", create=True)
            m_filename = p_filename[6:]
            m_output = m_fs.open(m_filename, 'w')
        elif p_filename.startswith('zip://'):
            m_fs = fs.open_fs(p_filename + ".zip",  create=True)
            m_filename = p_filename[6:]
            m_output = m_fs.open(m_filename, 'w')
        elif p_filename.startswith('kafka://'):
            if "KAFKA_SERVERS" in p_options:
                m_producer = KafkaProducer(bootstrap_servers=p_options["KAFKA_SERVERS"])
                m_topicname = p_filename[8:]
            else:
                raise SQLCliException("Please set KAFKA_SERVERS first")
        else:
            raise SQLCliException("Unknown file format.")

        m_row_struct = parse_formula_str(p_formula_str)
        for i in range(0, p_rows):
            if p_filename.startswith('kafka://'):
                m_producer.send(m_topicname, get_final_string(m_row_struct).encode())
            else:
                m_output.write(get_final_string(m_row_struct) + '\n')
        if p_filename.startswith('kafka://'):
            m_producer.flush()
        else:
            m_output.close()
    except Exception as e:
        raise SQLCliException(e)
    return
