# -*- coding: utf-8 -*-
import traceback

import fs
import re
import os
import random
from kafka import KafkaProducer
from .sqlcliexception import SQLCliException

# 缓存seed文件，来加速后面的随机函数random_from_seed工作
seed_cache = {"10s": [], "100s": [], "1Ks": [], "10Ks": [], "100Ks": [],"10n": [], "100n": [], "1Kn": [], "10Kn": []}


# 创建seed文件，默认在SQLCLI_HOME/data下
def Create_SeedCacheFile():
    if "SQLCLI_HOME" not in os.environ:
        raise SQLCliException("Missed SQLCLI_HOME, please set it first. Seed file will created in SQLCLI_HOME/data")

    # 如果不存在数据目录，创建它
    datadir = os.path.join(os.environ["SQLCLI_HOME"], "data")
    if not os.path.exists(datadir):
        os.makedirs(datadir)

    buf = []
    for n in range(1, 10):
        buf.append(random_ascii_letters_and_digits([100, ]) + "\n")
    seed_file = os.path.join(os.environ["SQLCLI_HOME"], "data", "seed_string.10s")
    with open(seed_file, 'w') as f:
        f.writelines(buf)

    buf = []
    for n in range(1, 100):
        buf.append(random_ascii_letters_and_digits([100, ]) + "\n")
    seed_file = os.path.join(os.environ["SQLCLI_HOME"], "data", "seed_string.100s")
    with open(seed_file, 'w') as f:
        f.writelines(buf)

    buf = []
    for n in range(1, 1000):
        buf.append(random_ascii_letters_and_digits([100, ]) + "\n")
    seed_file = os.path.join(os.environ["SQLCLI_HOME"], "data", "seed_string.1Ks")
    with open(seed_file, 'w') as f:
        f.writelines(buf)

    buf = []
    for n in range(1, 10 * 1000):
        buf.append(random_ascii_letters_and_digits([100, ]) + "\n")
    seed_file = os.path.join(os.environ["SQLCLI_HOME"], "data", "seed_string.10Ks")
    with open(seed_file, 'w') as f:
        f.writelines(buf)

    buf = []
    for n in range(1, 100 * 1000):
        buf.append(random_ascii_letters_and_digits([100, ]) + "\n")
    seed_file = os.path.join(os.environ["SQLCLI_HOME"], "data", "seed_string.100Ks")
    with open(seed_file, 'w') as f:
        f.writelines(buf)

    buf = []
    for n in range(1, 10):
        buf.append(random_digits([10, ]) + "\n")
    seed_file = os.path.join(os.environ["SQLCLI_HOME"], "data", "seed_int.10n")
    with open(seed_file, 'w') as f:
        f.writelines(buf)

    buf = []
    for n in range(1, 100):
        buf.append(random_digits([10, ]) + "\n")
    seed_file = os.path.join(os.environ["SQLCLI_HOME"], "data", "seed_int.100n")
    with open(seed_file, 'w') as f:
        f.writelines(buf)

    buf = []
    for n in range(1, 1000):
        buf.append(random_digits([10, ]) + "\n")
    seed_file = os.path.join(os.environ["SQLCLI_HOME"], "data", "seed_int.1Kn")
    with open(seed_file, 'w') as f:
        f.writelines(buf)

    buf = []
    for n in range(1, 10 * 1000):
        buf.append(random_digits([10, ]) + "\n")
    seed_file = os.path.join(os.environ["SQLCLI_HOME"], "data", "seed_int.10Kn")
    with open(seed_file, 'w') as f:
        f.writelines(buf)

# 缓存seed文件，默认在SQLCLI_HOME/data下
def Load_SeedCacheFile():
    if "SQLCLI_HOME" not in os.environ:
        print("Missed SQLCLI_HOME, please set it first. Seed file will created in SQLCLI_HOME/data")
        return

    seed_file = os.path.join(os.environ["SQLCLI_HOME"], "data", "seed_string.10")
    with open(seed_file, 'r') as f:
        seed_cache["10"] = f.readlines()

    seed_file = os.path.join(os.environ["SQLCLI_HOME"], "data", "seed_string.100")
    with open(seed_file, 'r') as f:
        seed_cache["100"] = f.readlines()

    seed_file = os.path.join(os.environ["SQLCLI_HOME"], "data", "seed_string.1K")
    with open(seed_file, 'r') as f:
        seed_cache["1K"] = f.readlines()

    seed_file = os.path.join(os.environ["SQLCLI_HOME"], "data", "seed_string.10K")
    with open(seed_file, 'r') as f:
        seed_cache["10K"] = f.readlines()

    seed_file = os.path.join(os.environ["SQLCLI_HOME"], "data", "seed_string.100K")
    with open(seed_file, 'r') as f:
        seed_cache["100K"] = f.readlines()

    seed_file = os.path.join(os.environ["SQLCLI_HOME"], "data", "seed_int.10")
    with open(seed_file, 'r') as f:
        seed_cache["10n"] = f.readlines()

    seed_file = os.path.join(os.environ["SQLCLI_HOME"], "data", "seed_int.100")
    with open(seed_file, 'r') as f:
        seed_cache["100n"] = f.readlines()

    seed_file = os.path.join(os.environ["SQLCLI_HOME"], "data", "seed_int.1K")
    with open(seed_file, 'r') as f:
        seed_cache["1Kn"] = f.readlines()

    seed_file = os.path.join(os.environ["SQLCLI_HOME"], "data", "seed_int.10K")
    with open(seed_file, 'r') as f:
        seed_cache["10Kn"] = f.readlines()

# 第一个参数是seed的名字
# 第二个参数是截取的最大长度
def random_from_seed(p_arg):
    m_SeedName = str(p_arg[0])
    m_nMaxLength = int(p_arg[1])
    if m_SeedName in seed_cache:
        n = len(seed_cache[m_SeedName])
        if n == 0:
            print("loading ...")
            Load_SeedCacheFile()
            n = len(seed_cache[m_SeedName])
        return seed_cache[m_SeedName][random.randint(0, n - 1)][0:m_nMaxLength]
    else:
        raise SQLCliException("Unknown seed [" + str(p_arg[0]) + "].  Please create it first.")


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
                     '|random_digits|identity|random_ascii_letters_and_digits|random_from_seed',
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
            if m_function_struct[0].upper() == "RANDOM_FROM_SEED":
                m_call_out_struct.append(random_from_seed)
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
            if p_options["KAFKA_SERVERS"] is None or len(p_options["KAFKA_SERVERS"].strip() == 0):
                raise SQLCliException("Please set KAFKA_SERVERS first")
            m_producer = KafkaProducer(bootstrap_servers=p_options["KAFKA_SERVERS"])
            m_topicname = p_filename[8:]
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
    except SQLCliException as e:
        raise SQLCliException(e.message)
    except Exception as e:
        raise SQLCliException(repr(e))
    return
