# -*- coding: utf-8 -*-
import re
import os
import time
import datetime
import random
from hdfs.client import Client, InsecureClient
from hdfs.util import HdfsError
import traceback
from glob import glob
import fs
from .sqlcliexception import SQLCliException


# 返回随机的Boolean类型
def random_boolean(p_arg):
    if p_arg:
        pass  # 这里用来避免p_arg参数无用的编译器警告
    return "1" if (random.randint(0, 1) == 1) else "0"


# 返回一个随机的时间
# 有3个参数：
#   stime   开始时间
#   etime   结束时间
#   frmt    日期格式，可以忽略，默认为"%Y-%m-%d"
def random_date(p_arg):
    if len(p_arg) > 2:
        frmt = str(p_arg[2])
    else:
        frmt = "%Y-%m-%d"
    try:
        stime = datetime.datetime.strptime(str(p_arg[0]), frmt)
        etime = datetime.datetime.strptime(str(p_arg[1]), frmt)
    except ValueError:
        raise SQLCliException("Invalid date format [" + str(frmt) + "] for [" + str(p_arg[0]) + "]")
    return (random.random() * (etime - stime) + stime).strftime(frmt)


# 返回一个随机的时间戳
# 有3个参数：
#   stime   开始时间
#   etime   结束时间
#   frmt    日期格式，可以忽略，默认为%H:%M:%S
def random_time(p_arg):
    if len(p_arg) > 2:
        frmt = "%Y-%m-%d " + str(p_arg[2])
    else:
        frmt = "%Y-%m-%d %H:%M:%S"
    try:
        stime = datetime.datetime.strptime("2000-01-01 " + str(p_arg[0]), frmt)
        etime = datetime.datetime.strptime("2000-01-01 " + str(p_arg[1]), frmt)
    except ValueError:
        raise SQLCliException("Invalid timestamp format [" + str(frmt) + "] for [" + str(p_arg[0]) + "]")
    return (random.random() * (etime - stime) + stime).strftime(frmt)[11:]


# 返回一个随机的时间戳
# 有3个参数：
#   stime   开始时间
#   etime   结束时间
#   frmt    日期格式，可以忽略，默认为%Y-%m-%d %H:%M:%S
def random_timestamp(p_arg):
    if len(p_arg) > 2:
        frmt = str(p_arg[2])
    else:
        frmt = "%Y-%m-%d %H:%M:%S"
    try:
        stime = datetime.datetime.strptime(str(p_arg[0]), frmt)
        etime = datetime.datetime.strptime(str(p_arg[1]), frmt)
    except ValueError:
        raise SQLCliException("Invalid timestamp format [" + str(frmt) + "] for [" + str(p_arg[0]) + "]")
    return (random.random() * (etime - stime) + stime).strftime(frmt)


# 返回系统当前时间,Unix时间戳方式
def current_unixtimestamp(p_arg):
    if p_arg:
        pass
    return str(int(time.mktime(datetime.datetime.now().timetuple())))


# 返回系统当前时间
def current_timestamp(p_arg):
    if len(p_arg) == 1:
        frmt = str(p_arg[0]).strip()
        if len(frmt) == 0:
            frmt = "%Y-%m-%d %H:%M:%S"
    else:
        frmt = "%Y-%m-%d %H:%M:%S"
    return datetime.datetime.now().strftime(frmt)


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


# 返回一个自增长序列
# 有2个参数：
#   identity_name   自增序列号的名字
#   start           即自正常序列的开始值
#   步长目前没有支持，都是1
def identity(p_arg):
    identity_name = str(p_arg[0])
    start = int(p_arg[1])
    if not hasattr(identity, 'x'):
        identity.x = {}
    if identity_name not in identity.x.keys():
        identity.x[identity_name] = start
    else:
        identity.x[identity_name] = identity.x[identity_name] + 1
    return str(identity.x[identity_name])


# 返回一个自增长的时间戳
# 有4个参数：
#   identity_name   自增序列号的名字
#   stime   开始时间
#     若stime写作current_timestamp, 则自增上从当前时间开始
#   frmt    日期格式， 如%Y-%m-%d %H:%M:%S
#   step    步长，可以用ms,us,s来表示，默认情况下是ms
def identity_timestamp(p_arg):
    identity_name = str(p_arg[0])
    ptime = str(p_arg[1])
    frmt = str(p_arg[2])
    step = str(p_arg[3])
    if not hasattr(identity_timestamp, 'x'):
        identity_timestamp.x = {}
    if identity_name not in identity_timestamp.x.keys():
        if ptime == "current_timestamp":
            identity_timestamp.x[identity_name] = datetime.datetime.now()
        else:
            identity_timestamp.x[identity_name] = datetime.datetime.strptime(ptime, frmt)
    else:
        # 判断步长单位，默认是毫秒，可以是s,ms
        if step.endswith("s"):
            if step.endswith("ms"):
                # end with ms 毫秒
                identity_timestamp.x[identity_name] = (identity_timestamp.x[identity_name] +
                                                       datetime.timedelta(milliseconds=float(step[:-2])))
            elif step.endswith("us"):
                # end with us 微妙
                identity_timestamp.x[identity_name] = (identity_timestamp.x[identity_name] +
                                                       datetime.timedelta(microseconds=float(step[:-2])))
            else:
                # end with s 秒
                identity_timestamp.x[identity_name] = (identity_timestamp.x[identity_name] +
                                                       datetime.timedelta(seconds=float(step[:-1])))
        else:
            identity_timestamp.x[identity_name] = (identity_timestamp.x[identity_name] +
                                                   datetime.timedelta(milliseconds=float(step)))
    return identity_timestamp.x[identity_name].strftime(frmt)


class MT19937:
    def __init__(self, seed):
        self.mt = [0] * 624
        self.mt[0] = seed
        self.mti = 0
        for i in range(1, 624):
            self.mt[i] = self._int32(1812433253 * (self.mt[i - 1] ^ self.mt[i - 1] >> 30) + i)
        self.twist()

    @staticmethod
    def _int32(x):
        return int(0xFFFFFFFF & x)

    def extract_number(self):
        y = self.mt[self.mti]
        y = y ^ y >> 11
        y = y ^ y << 7 & 2636928640
        y = y ^ y << 15 & 4022730752
        y = y ^ y >> 18
        self.mti = (self.mti + 1) % 624
        return self._int32(y)

    def twist(self):
        for i in range(0, 624):
            y = self._int32((self.mt[i] & 0x80000000) + (self.mt[(i + 1) % 624] & 0x7fffffff))
            self.mt[i] = (y >> 1) ^ self.mt[(i + 397) % 624]

            if y % 2 != 0:
                self.mt[i] = self.mt[i] ^ 0x9908b0df


class DataWrapper(object):
    def __init__(self):
        self.c_SeedFileDir = None
        # 缓存seed文件，来加速后面的随机函数random_from_seed工作
        self.seed_cache = {}
        # 全局内存文件系统句柄
        self.g_MemoryFSHandler = None
        self.c_HDFS_ConnectedUser = None
        self.SQLOptions = None  # 程序处理参数

    # 创建seed文件，默认在SQLCLI_HOME/data下
    def create_seed_cache(self,
                          data_type,  # 种子文件的数据类型，目前支持String和integer
                          data_length,  # 每个随机数的最大数据长度
                          row_count,  # 种子文件的行数，即随机数的数量
                          seed_name,
                          null_count=0):

        if self.c_SeedFileDir is None:
            if "SQLCLI_HOME" not in os.environ:
                raise SQLCliException(
                    "Missed SQLCLI_HOME, please set it first. Seed file will created in SQLCLI_HOME/data")
            self.c_SeedFileDir = os.path.join(os.environ["SQLCLI_HOME"], "data")
        # 如果不存在数据目录，创建它
        if not os.path.exists(self.c_SeedFileDir):
            os.makedirs(self.c_SeedFileDir)

        # 检查输入的参数
        if data_type.upper() not in ('STRING', 'INTEGER'):
            raise SQLCliException("Data Type must be String or integer.")
        if data_length <= 0:
            raise SQLCliException("Data Length must great then zero.")
        if row_count <= 0:
            raise SQLCliException("rows must great then zero.")

        buf = []
        if data_type.upper() == "STRING":
            for n in range(0, row_count):
                buf.append(random_ascii_letters_and_digits([data_length, ]) + "\n")
        if data_type.upper() == "INTEGER":
            for n in range(0, row_count):
                buf.append(random_digits([data_length, ]) + "\n")
        if null_count > 0:
            for n in range(0, null_count):
                pos = random.randint(0, row_count - 1)
                buf[pos] = "\n"
        seed_file = os.path.join(self.c_SeedFileDir, seed_name + ".seed")
        with open(seed_file, 'w') as f:
            f.writelines(buf)

    # 缓存seed文件，默认在SQLCLI_HOME/data下
    def load_seed_ache(self):
        if self.c_SeedFileDir is None:
            if "SQLCLI_HOME" not in os.environ:
                self.c_SeedFileDir = os.getcwd()
            else:
                self.c_SeedFileDir = os.path.join(os.environ["SQLCLI_HOME"], "data")
        seed_path = os.path.join(self.c_SeedFileDir, "*.seed")
        for seed_file in glob(seed_path):
            seed_name = os.path.basename(seed_file)[:-5]  # 去掉.seed的后缀
            with open(seed_file, 'r', encoding="utf-8") as f:
                seed_lines = f.readlines()
            for pos in range(0, len(seed_lines)):
                if seed_lines[pos].endswith("\n"):
                    seed_lines[pos] = seed_lines[pos][:-1]  # 去掉回车换行
            self.seed_cache[seed_name] = seed_lines

    # 第一个参数是seed的名字,
    # 第二个参数是开始截取的位置，可以省略。 如果指定，这里的开始位置从0开始计算
    # 第三个参数是截取的最大长度
    def random_from_seed(self, p_arg):
        seed_name = str(p_arg[0])
        if len(p_arg) == 3:
            # 从指定位置开始截取数据内容
            start_pos = int(p_arg[1])
            max_length = int(p_arg[2])
        elif len(p_arg) == 2:
            # 从头开始截取文件内容
            start_pos = 0
            max_length = int(p_arg[1])
        else:
            start_pos = 0
            max_length = -1

        # 如果还没有加载种子，就先尝试加载
        if seed_name not in self.seed_cache:
            self.load_seed_ache()

        # 在种子中查找需要的内容
        if seed_name in self.seed_cache:
            n = len(self.seed_cache[seed_name])
            if n == 0:
                # 空的Seed文件，可能是由于Seed文件刚刚创建，则尝试重新加载数据
                self.load_seed_ache()
                n = len(self.seed_cache[seed_name])
                if n == 0:
                    raise SQLCliException("Seed cache is zero. [" + str(p_arg[0]) + "].")
            random_lines = self.seed_cache[seed_name][random.randint(0, n - 1)]
            if max_length == -1:
                return random_lines[start_pos:]
            else:
                return random_lines[start_pos:start_pos + max_length]
        else:
            raise SQLCliException("Unknown seed [" + str(p_arg[0]) + "].  Please create it first.")

    # 将传递的SQL字符串转换成一个带有函数指针的数组
    def parse_formula_str(self, p_formula_str):
        # 首先按照{}来把所有的函数定义分拆到数组中
        m_row_struct = re.split('[{}]', p_formula_str)
        m_return_row_struct = []

        # 在分拆的数组中依次查找关键字，作为后续函数替换的需要
        for m_nRowPos in range(0, len(m_row_struct)):
            if re.search('random_ascii_lowercase|random_ascii_uppercase|random_ascii_letters' +
                         '|random_digits|identity|identity_timestamp|random_ascii_letters_and_digits|random_from_seed' +
                         '|random_date|random_timestamp|random_time|random_boolean|'
                         'current_timestamp|current_unixtimestamp|value',
                         m_row_struct[m_nRowPos], re.IGNORECASE):
                m_function_struct = re.split(r'[(,)]', m_row_struct[m_nRowPos])
                for pos in range(0, len(m_function_struct)):
                    m_function_struct[pos] = m_function_struct[pos].strip()
                    if m_function_struct[pos].startswith("'"):
                        m_function_struct[pos] = m_function_struct[pos][1:]
                    if m_function_struct[pos].endswith("'"):
                        m_function_struct[pos] = m_function_struct[pos][0:-1]
                if len(m_function_struct[-1]) == 0:
                    m_function_struct.pop()
                m_call_out_struct = []
                if m_function_struct[0].upper() == "RANDOM_ASCII_LOWERCASE":
                    m_call_out_struct.append(random_ascii_lowercase)
                    m_call_out_struct.append(m_function_struct[1:])
                    m_call_out_struct.append("__NO_NAME__")
                elif re.search(r"(.*):RANDOM_ASCII_LOWERCASE", m_function_struct[0].upper()):
                    match_obj = re.search(r"(.*):RANDOM_ASCII_LOWERCASE", m_function_struct[0].upper())
                    column_name = match_obj.group(1).upper().strip()
                    # 检查列名是否已经定义
                    found = False
                    for row in m_return_row_struct:
                        if isinstance(row, list):
                            if row[2] == column_name:
                                found = True
                                break
                    if found:
                        raise SQLCliException("Invalid pattern. "
                                              "Please make sure column [" + column_name + "] is not duplicate.")
                    m_call_out_struct.append(random_ascii_lowercase)
                    m_call_out_struct.append(m_function_struct[1:])
                    m_call_out_struct.append(column_name)
                elif m_function_struct[0].upper() == "RANDOM_ASCII_UPPERCASE":
                    m_call_out_struct.append(random_ascii_uppercase)
                    m_call_out_struct.append(m_function_struct[1:])
                    m_call_out_struct.append("__NO_NAME__")
                elif re.search(r"(.*):RANDOM_ASCII_UPPERCASE", m_function_struct[0].upper()):
                    match_obj = re.search(r"(.*):RANDOM_ASCII_UPPERCASE", m_function_struct[0].upper())
                    column_name = match_obj.group(1).upper().strip()
                    # 检查列名是否已经定义
                    found = False
                    for row in m_return_row_struct:
                        if isinstance(row, list):
                            if row[2] == column_name:
                                found = True
                                break
                    if found:
                        raise SQLCliException("Invalid pattern. "
                                              "Please make sure column [" + column_name + "] is not duplicate.")
                    m_call_out_struct.append(random_ascii_uppercase)
                    m_call_out_struct.append(m_function_struct[1:])
                    m_call_out_struct.append(column_name)
                elif m_function_struct[0].upper() == "RANDOM_ASCII_LETTERS":
                    m_call_out_struct.append(random_ascii_letters)
                    m_call_out_struct.append(m_function_struct[1:])
                    m_call_out_struct.append("__NO_NAME__")
                elif re.search(r"(.*):RANDOM_ASCII_LETTERS", m_function_struct[0].upper()):
                    match_obj = re.search(r"(.*):RANDOM_ASCII_LETTERS", m_function_struct[0].upper())
                    column_name = match_obj.group(1).upper().strip()
                    # 检查列名是否已经定义
                    found = False
                    for row in m_return_row_struct:
                        if isinstance(row, list):
                            if row[2] == column_name:
                                found = True
                                break
                    if found:
                        raise SQLCliException("Invalid pattern. "
                                              "Please make sure column [" + column_name + "] is not duplicate.")
                    m_call_out_struct.append(random_ascii_letters)
                    m_call_out_struct.append(m_function_struct[1:])
                    m_call_out_struct.append(column_name)
                elif m_function_struct[0].upper() == "RANDOM_DIGITS":
                    m_call_out_struct.append(random_digits)
                    m_call_out_struct.append(m_function_struct[1:])
                    m_call_out_struct.append("__NO_NAME__")
                elif re.search(r"(.*):RANDOM_DIGITS", m_function_struct[0].upper()):
                    match_obj = re.search(r"(.*):RANDOM_DIGITS", m_function_struct[0].upper())
                    column_name = match_obj.group(1).upper().strip()
                    # 检查列名是否已经定义
                    found = False
                    for row in m_return_row_struct:
                        if isinstance(row, list):
                            if row[2] == column_name:
                                found = True
                                break
                    if found:
                        raise SQLCliException("Invalid pattern. "
                                              "Please make sure column [" + column_name + "] is not duplicate.")
                    m_call_out_struct.append(random_digits)
                    m_call_out_struct.append(m_function_struct[1:])
                    m_call_out_struct.append(column_name)
                elif m_function_struct[0].upper() == "IDENTITY_TIMESTAMP":
                    m_call_out_struct.append(identity_timestamp)
                    # 如果没有列名，第一个参数信息是identity#加上列号的信息
                    if len(m_function_struct) == 5:
                        m_function_struct = m_function_struct[1:]
                    elif len(m_function_struct) == 4:
                        m_function_struct[0] = "identity#" + str(m_nRowPos)
                    else:
                        raise SQLCliException("Invalid pattern. "
                                              "Please make sure IDENTITY_TIMESTAMP has correct parameters.")
                    m_call_out_struct.append(m_function_struct)
                    m_call_out_struct.append("__NO_NAME__")
                elif re.search(r"(.*):IDENTITY_TIMESTAMP", m_function_struct[0].upper()):
                    match_obj = re.search(r"(.*):IDENTITY_TIMESTAMP", m_function_struct[0].upper())
                    column_name = match_obj.group(1).upper().strip()
                    # 检查列名是否已经定义
                    found = False
                    for row in m_return_row_struct:
                        if isinstance(row, list):
                            if row[2] == column_name:
                                found = True
                                break
                    if found:
                        raise SQLCliException("Invalid pattern. "
                                              "Please make sure column [" + column_name + "] is not duplicate.")
                    m_call_out_struct.append(identity_timestamp)
                    # 如果有列名，第一个参数信息是identity#加上列名的信息
                    m_function_struct[0] = "identity#" + column_name
                    m_call_out_struct.append(m_function_struct)
                    m_call_out_struct.append(column_name)
                elif m_function_struct[0].upper() == "IDENTITY":
                    m_call_out_struct.append(identity)
                    # 如果没有列名，第一个参数信息是identity#加上列号的信息
                    m_function_struct[0] = "identity#" + str(m_nRowPos)
                    m_call_out_struct.append(m_function_struct)
                    m_call_out_struct.append("__NO_NAME__")
                elif re.search(r"(.*):IDENTITY", m_function_struct[0].upper()):
                    match_obj = re.search(r"(.*):IDENTITY", m_function_struct[0].upper())
                    column_name = match_obj.group(1).upper().strip()
                    # 检查列名是否已经定义
                    found = False
                    for row in m_return_row_struct:
                        if isinstance(row, list):
                            if row[2] == column_name:
                                found = True
                                break
                    if found:
                        raise SQLCliException("Invalid pattern. "
                                              "Please make sure column [" + column_name + "] is not duplicate.")
                    m_call_out_struct.append(identity)
                    # 如果有列名，第一个参数信息是identity#加上列名的信息
                    m_function_struct[0] = "identity#" + column_name
                    m_call_out_struct.append(m_function_struct)
                    m_call_out_struct.append(column_name)
                elif m_function_struct[0].upper() == "RANDOM_ASCII_LETTERS_AND_DIGITS":
                    m_call_out_struct.append(random_ascii_letters_and_digits)
                    m_call_out_struct.append(m_function_struct[1:])
                    m_call_out_struct.append("__NO_NAME__")
                elif re.search(r"(.*):RANDOM_ASCII_LETTERS_AND_DIGITS", m_function_struct[0].upper()):
                    match_obj = re.search(r"(.*):RANDOM_ASCII_LETTERS_AND_DIGITS", m_function_struct[0].upper())
                    column_name = match_obj.group(1).upper().strip()
                    # 检查列名是否已经定义
                    found = False
                    for row in m_return_row_struct:
                        if isinstance(row, list):
                            if row[2] == column_name:
                                found = True
                                break
                    if found:
                        raise SQLCliException("Invalid pattern. "
                                              "Please make sure column [" + column_name + "] is not duplicate.")
                    m_call_out_struct.append(random_ascii_letters_and_digits)
                    m_call_out_struct.append(m_function_struct[1:])
                    m_call_out_struct.append(column_name)
                elif m_function_struct[0].upper() == "RANDOM_FROM_SEED":
                    m_call_out_struct.append(self.random_from_seed)
                    m_call_out_struct.append(m_function_struct[1:])
                    m_call_out_struct.append("__NO_NAME__")
                elif re.search(r"(.*):RANDOM_FROM_SEED", m_function_struct[0].upper()):
                    match_obj = re.search(r"(.*):RANDOM_FROM_SEED", m_function_struct[0].upper())
                    column_name = match_obj.group(1).upper().strip()
                    # 检查列名是否已经定义
                    found = False
                    for row in m_return_row_struct:
                        if isinstance(row, list):
                            if row[2] == column_name:
                                found = True
                                break
                    if found:
                        raise SQLCliException("Invalid pattern. "
                                              "Please make sure column [" + column_name + "] is not duplicate.")
                    m_call_out_struct.append(self.random_from_seed)
                    m_call_out_struct.append(m_function_struct[1:])
                    m_call_out_struct.append(column_name)
                elif m_function_struct[0].upper() == "RANDOM_DATE":
                    m_call_out_struct.append(random_date)
                    m_call_out_struct.append(m_function_struct[1:])
                    m_call_out_struct.append("__NO_NAME__")
                elif re.search(r"(.*):RANDOM_DATE", m_function_struct[0].upper()):
                    match_obj = re.search(r"(.*):RANDOM_DATE", m_function_struct[0].upper())
                    column_name = match_obj.group(1).upper().strip()
                    # 检查列名是否已经定义
                    found = False
                    for row in m_return_row_struct:
                        if isinstance(row, list):
                            if row[2] == column_name:
                                found = True
                                break
                    if found:
                        raise SQLCliException("Invalid pattern. "
                                              "Please make sure column [" + column_name + "] is not duplicate.")
                    m_call_out_struct.append(random_date)
                    m_call_out_struct.append(m_function_struct[1:])
                    m_call_out_struct.append(column_name)
                elif m_function_struct[0].upper() == "RANDOM_TIMESTAMP":
                    m_call_out_struct.append(random_timestamp)
                    m_call_out_struct.append(m_function_struct[1:])
                    m_call_out_struct.append("__NO_NAME__")
                elif re.search(r"(.*):RANDOM_TIMESTAMP", m_function_struct[0].upper()):
                    match_obj = re.search(r"(.*):RANDOM_TIMESTAMP", m_function_struct[0].upper())
                    column_name = match_obj.group(1).upper().strip()
                    # 检查列名是否已经定义
                    found = False
                    for row in m_return_row_struct:
                        if isinstance(row, list):
                            if row[2] == column_name:
                                found = True
                                break
                    if found:
                        raise SQLCliException("Invalid pattern. "
                                              "Please make sure column [" + column_name + "] is not duplicate.")
                    m_call_out_struct.append(random_timestamp)
                    m_call_out_struct.append(m_function_struct[1:])
                    m_call_out_struct.append(column_name)
                elif m_function_struct[0].upper() == "RANDOM_TIME":
                    m_call_out_struct.append(random_time)
                    m_call_out_struct.append(m_function_struct[1:])
                    m_call_out_struct.append("__NO_NAME__")
                elif re.search(r"(.*):RANDOM_TIME", m_function_struct[0].upper()):
                    match_obj = re.search(r"(.*):RANDOM_TIME", m_function_struct[0].upper())
                    column_name = match_obj.group(1).upper().strip()
                    # 检查列名是否已经定义
                    found = False
                    for row in m_return_row_struct:
                        if isinstance(row, list):
                            if row[2] == column_name:
                                found = True
                                break
                    if found:
                        raise SQLCliException("Invalid pattern. "
                                              "Please make sure column [" + column_name + "] is not duplicate.")
                    m_call_out_struct.append(random_time)
                    m_call_out_struct.append(m_function_struct[1:])
                    m_call_out_struct.append(column_name)
                elif m_function_struct[0].upper() == "RANDOM_BOOLEAN":
                    m_call_out_struct.append(random_boolean)
                    m_call_out_struct.append(m_function_struct[1:])
                    m_call_out_struct.append("__NO_NAME__")
                elif re.search(r"(.*):RANDOM_BOOLEAN", m_function_struct[0].upper()):
                    match_obj = re.search(r"(.*):RANDOM_BOOLEAN", m_function_struct[0].upper())
                    column_name = match_obj.group(1).upper().strip()
                    # 检查列名是否已经定义
                    found = False
                    for row in m_return_row_struct:
                        if isinstance(row, list):
                            if row[2] == column_name:
                                found = True
                                break
                    if found:
                        raise SQLCliException("Invalid pattern. "
                                              "Please make sure column [" + column_name + "] is not duplicate.")
                    m_call_out_struct.append(random_boolean)
                    m_call_out_struct.append(m_function_struct[1:])
                    m_call_out_struct.append(column_name)
                elif m_function_struct[0].upper() == "CURRENT_TIMESTAMP":
                    m_call_out_struct.append(current_timestamp)
                    m_call_out_struct.append(m_function_struct[1:])
                    m_call_out_struct.append("__NO_NAME__")
                elif re.search(r"(.*):CURRENT_TIMESTAMP", m_function_struct[0].upper()):
                    match_obj = re.search(r"(.*):CURRENT_TIMESTAMP", m_function_struct[0].upper())
                    column_name = match_obj.group(1).upper().strip()
                    # 检查列名是否已经定义
                    found = False
                    for row in m_return_row_struct:
                        if isinstance(row, list):
                            if row[2] == column_name:
                                found = True
                                break
                    if found:
                        raise SQLCliException("Invalid pattern. "
                                              "Please make sure column [" + column_name + "] is not duplicate.")
                    m_call_out_struct.append(current_timestamp)
                    m_call_out_struct.append(m_function_struct[1:])
                    m_call_out_struct.append(column_name)
                elif m_function_struct[0].upper() == "CURRENT_UNIXTIMESTAMP":
                    m_call_out_struct.append(current_unixtimestamp)
                    m_call_out_struct.append(m_function_struct[1:])
                    m_call_out_struct.append("__NO_NAME__")
                elif re.search(r"(.*):CURRENT_UNIXTIMESTAMP", m_function_struct[0].upper()):
                    match_obj = re.search(r"(.*):CURRENT_UNIXTIMESTAMP", m_function_struct[0].upper())
                    column_name = match_obj.group(1).upper().strip()
                    # 检查列名是否已经定义
                    found = False
                    for row in m_return_row_struct:
                        if isinstance(row, list):
                            if row[2] == column_name:
                                found = True
                                break
                    if found:
                        raise SQLCliException("Invalid pattern. "
                                              "Please make sure column [" + column_name + "] is not duplicate.")
                    m_call_out_struct.append(current_unixtimestamp)
                    m_call_out_struct.append(m_function_struct[1:])
                    m_call_out_struct.append(column_name)
                elif m_function_struct[0].upper() == "VALUE":
                    # 必须用：开头来表示字段名称
                    if not m_function_struct[1:][0].startswith(":"):
                        raise SQLCliException("Invalid pattern. Please use Value(:ColumnName).")
                    else:
                        column_name = m_function_struct[1:][0][1:]
                    # 检查列名是否已经定义
                    found = False
                    for row in m_return_row_struct:
                        if isinstance(row, list):
                            if row[2] == column_name.upper():
                                found = True
                                break
                    if not found:
                        m_ValidColumn = ""
                        for row in m_return_row_struct:
                            if isinstance(row, list):
                                m_ValidColumn = m_ValidColumn + row[2] + "|"
                        raise SQLCliException("Invalid pattern. "
                                              "Please make sure column [" + column_name + "] is valid.\n" +
                                              "Valid Column=[" + m_ValidColumn + "]")
                    m_call_out_struct.append(None)
                    m_call_out_struct.append(column_name)
                    m_call_out_struct.append("VALUE")
                elif re.search(r"(.*):VALUE", m_function_struct[0].upper()):
                    match_obj = re.search(r"(.*):VALUE", m_function_struct[0].upper())
                    column_name = match_obj.group(1).upper().strip()
                    # 检查列名是否已经定义
                    found = False
                    for row in m_return_row_struct:
                        if isinstance(row, list):
                            if row[2] == column_name:
                                found = True
                                break
                    if found:
                        raise SQLCliException("Invalid pattern. "
                                              "Please make sure column [" + column_name + "] is not duplicate.")
                    # 必须用：开头来表示字段名称
                    if not m_function_struct[1:][0].startswith(":"):
                        raise SQLCliException("Invalid pattern. Please use Value(:ColumnName).")
                    else:
                        column_name = m_function_struct[1:][0][1:]
                    # 检查列名是否已经定义
                    found = False
                    for row in m_return_row_struct:
                        if isinstance(row, list):
                            if row[2] == column_name.upper():
                                found = True
                                break
                    if not found:
                        m_ValidColumn = ""
                        for row in m_return_row_struct:
                            if isinstance(row, list):
                                m_ValidColumn = m_ValidColumn + row[2] + "|"
                        raise SQLCliException("Invalid pattern. "
                                              "Please make sure column [" + column_name + "] is valid.\n" +
                                              "Valid Column=[" + m_ValidColumn + "]")
                    m_call_out_struct.append(None)
                    m_call_out_struct.append(column_name)
                    m_call_out_struct.append("VALUE")
                else:
                    raise SQLCliException("Invalid pattern. parse [" + m_row_struct[m_nRowPos] + "] failed. ")
                m_return_row_struct.append(m_call_out_struct)
            else:
                m_return_row_struct.append(m_row_struct[m_nRowPos])
        return m_return_row_struct

    def get_final_string(self, p_row_struct):
        if self:
            pass
        m_Result = ""
        m_Saved_ColumnData = {}
        for col in p_row_struct:
            if isinstance(col, list):
                if col[2] == "VALUE":
                    column_name = col[1]
                    m_Value = m_Saved_ColumnData[column_name.upper()]
                    m_Result = m_Result + m_Value
                elif col[2] == "__NO_NAME__":
                    m_Result = m_Result + col[0](col[1])
                else:
                    m_Value = col[0](col[1])  # 根据函数指针计算返回后的实际内容
                    m_Saved_ColumnData[col[2]] = m_Value
                    m_Result = m_Result + m_Value
            else:
                m_Result = m_Result + col
        return m_Result

    def Convert_file(self, p_srcfileType, p_srcfilename, p_dstfileType, p_dstfilename):
        try:
            if p_srcfileType.upper() == "MEM":
                m_srcFSType = "MEM"
                if self.g_MemoryFSHandler is None:
                    self.g_MemoryFSHandler = fs.open_fs('mem://')
                    m_srcFS = self.g_MemoryFSHandler
                else:
                    m_srcFS = self.g_MemoryFSHandler
                m_srcFileName = p_srcfilename
            elif p_srcfileType.upper() == "FS":
                m_srcFSType = "FS"
                m_srcFS = fs.open_fs('./')
                m_srcFileName = p_srcfilename
            elif p_srcfileType.upper() == "HDFS":
                m_srcFSType = "HDFS"
                m_srcFullFileName = p_srcfilename
                m_Protocal = m_srcFullFileName.split("://")[0]
                m_NodePort = m_srcFullFileName[len(m_Protocal) + 3:].split("/")[0]
                m_WebFSURL = m_Protocal + "://" + m_NodePort
                m_WebFSDir, m_srcFileName = os.path.split(m_srcFullFileName[len(m_WebFSURL):])
                m_srcFS = Client(m_WebFSURL, m_WebFSDir, proxy=None, session=None)
            else:
                m_srcFS = None
                m_srcFileName = None
                m_srcFSType = "Not Supported"

            if p_dstfileType.upper() == "MEM":
                m_dstFSType = "MEM"
                if self.g_MemoryFSHandler is None:
                    self.g_MemoryFSHandler = fs.open_fs('mem://')
                    m_dstFS = self.g_MemoryFSHandler
                else:
                    m_dstFS = self.g_MemoryFSHandler
                m_dstFileName = p_dstfilename
            elif p_dstfileType.upper() == "FS":
                m_dstFSType = "FS"
                m_dstFS = fs.open_fs('./')
                m_dstFileName = p_dstfilename
            elif p_dstfileType.upper() == "HDFS":
                m_dstFSType = "HDFS"
                m_dstFullFileName = p_dstfilename
                m_Protocal = m_dstFullFileName.split("://")[0]
                m_NodePort = m_dstFullFileName[len(m_Protocal) + 3:].split("/")[0]
                m_WebFSURL = m_Protocal + "://" + m_NodePort
                m_WebFSDir, m_dstFileName = os.path.split(m_dstFullFileName[len(m_WebFSURL):])
                m_dstFS = Client(m_WebFSURL, m_WebFSDir, proxy=None, session=None)
            else:
                m_dstFS = None
                m_dstFileName = None
                m_dstFSType = "Not Supported convert."

            if m_srcFSType == "Not Supported" or m_dstFSType == "Not Supported":
                raise SQLCliException("Not supported convert. From [" + p_srcfileType + "] to [" + p_dstfileType + "]")

            if m_srcFSType in ('MEM', 'FS') and m_dstFSType in ('MEM', 'FS'):
                with m_srcFS.openbin(m_srcFileName, "r") as m_reader, m_dstFS.openbin(m_dstFileName, "w") as m_writer:
                    while True:
                        m_Contents = m_reader.read(8192)
                        if len(m_Contents) == 0:
                            break
                        m_writer.write(m_Contents)

            if m_srcFSType == "HDFS" and m_dstFSType in ('MEM', 'FS'):
                with m_srcFS.read(m_srcFileName, "rb") as m_reader, m_dstFS.openbin(m_dstFileName, "w") as m_writer:
                    while True:
                        m_Contents = m_reader.read(8192)
                        if len(m_Contents) == 0:
                            break
                        m_writer.write(m_Contents)

            # 对于HDFS的写入，每80M提交一次，以避免内存的OOM问题
            if m_srcFSType in ('MEM', 'FS') and m_dstFSType == "HDFS":
                bHeaderWrite = True
                with m_srcFS.openbin(m_srcFileName, "r") as m_reader:
                    while True:
                        m_Contents = m_reader.read(8192 * 10240)
                        if len(m_Contents) == 0:
                            break
                        if bHeaderWrite:
                            with m_dstFS.write(m_dstFileName, overwrite=True) as m_writer:
                                m_writer.write(m_Contents)
                            bHeaderWrite = False
                        else:
                            with m_dstFS.write(m_dstFileName, append=True) as m_writer:
                                m_writer.write(m_Contents)

            if m_srcFSType == "HDFS" and m_dstFSType == "HDFS":
                bHeaderWrite = True
                with m_srcFS.read(m_srcFileName) as m_reader:
                    while True:
                        m_Contents = m_reader.read(8192 * 10240)
                        if len(m_Contents) == 0:
                            break
                        if bHeaderWrite:
                            with m_dstFS.write(m_dstFileName, overwrite=True) as m_writer:
                                m_writer.write(m_Contents)
                            bHeaderWrite = False
                        else:
                            with m_dstFS.write(m_dstFileName, append=True) as m_writer:
                                m_writer.write(m_Contents)
        except HdfsError as he:
            # HDFS 会打印整个堆栈信息，所以这里默认只打印第一行的信息
            if "SQLCLI_DEBUG" in os.environ:
                raise SQLCliException(he.message)
            else:
                raise SQLCliException(he.message.split('\n')[0])

    def Create_file(self, p_filetype, p_filename, p_formula_str, p_rows, p_encoding='UTF-8'):
        try:
            m_output = None
            m_HDFS_Handler = None

            if p_filetype.upper() == "MEM":
                m_fs = fs.open_fs('mem://')
                m_filename = p_filename
                m_output = m_fs.open(path=m_filename, mode='w', encoding=p_encoding)
            elif p_filetype.upper() == "FS":
                m_filename = p_filename
                m_output = open(file=m_filename, mode='w', encoding=p_encoding)
            elif p_filetype.upper() == "HDFS":
                # HDFS 文件格式： http://node:port/xx/yy/cc.dat
                # 注意这里的node和port都是webfs端口，不是rpc端口
                m_Protocal = p_filename.split("://")[0]
                m_NodePort = p_filename[len(m_Protocal) + 3:].split("/")[0]
                m_WebFSURL = m_Protocal + "://" + m_NodePort
                m_WebFSDir, m_filename = os.path.split(p_filename[len(m_WebFSURL):])
                if self.c_HDFS_ConnectedUser is None:
                    m_HDFS_Handler = Client(url=m_WebFSURL,
                                            root=m_WebFSDir,
                                            proxy=None,
                                            session=None)
                else:
                    m_HDFS_Handler = InsecureClient(url=m_WebFSURL,
                                                    user=self.c_HDFS_ConnectedUser,
                                                    root=m_WebFSDir)
            else:
                raise SQLCliException("Unknown file format.")

            m_row_struct = self.parse_formula_str(p_formula_str)
            buf = []
            if p_filetype.upper() == "HDFS":  # 处理HDFS文件写入
                # 总是覆盖服务器上的文件, 每10W行提交一次服务器文件, 以避免内存的OOM问题
                if p_rows < 100000:
                    with m_HDFS_Handler.write(hdfs_path=m_filename, overwrite=True) as m_output:
                        for i in range(0, p_rows):
                            m_output.write((self.get_final_string(m_row_struct) + "\n").encode())
                else:
                    for i in range(0, p_rows // 100000):
                        if i == 0:
                            with m_HDFS_Handler.write(hdfs_path=m_filename, overwrite=True) as m_output:
                                for j in range(0, 100000):
                                    m_output.write((self.get_final_string(m_row_struct) + "\n").encode())
                        else:
                            with m_HDFS_Handler.write(hdfs_path=m_filename, append=True) as m_output:
                                for j in range(0, 100000):
                                    m_output.write((self.get_final_string(m_row_struct) + "\n").encode())
                    with m_HDFS_Handler.write(hdfs_path=m_filename, append=True) as m_output:
                        for i in range(0, p_rows % 100000):
                            m_output.write((self.get_final_string(m_row_struct) + "\n").encode())
            else:  # 处理普通文件写入
                for i in range(0, p_rows):
                    buf.append(self.get_final_string(m_row_struct) + '\n')
                    if len(buf) == 100000:  # 为了提高IO效率，每10W条写入文件一次
                        m_output.writelines(buf)
                        buf = []
                if len(buf) != 0:
                    m_output.writelines(buf)
                m_output.close()

                # 重置identity的序列号，保证下次从开头开始
                if hasattr(identity, 'x'):
                    delattr(identity, 'x')
                if hasattr(identity_timestamp, 'x'):
                    delattr(identity_timestamp, 'x')
        except SQLCliException as e:
            raise SQLCliException(e.message)
        except Exception as e:
            if "SQLCLI_DEBUG" in os.environ:
                print('traceback.print_exc():\n%s' % traceback.print_exc())
                print('traceback.format_exc():\n%s' % traceback.format_exc())
            raise SQLCliException(repr(e))

    def Process_SQLCommand(self, p_szSQL):
        # 设置数据种子文件读取的位置，如果不设置，默认是SQLCLI_HOME\data
        match_obj = re.match(r"data\s+set\s+seedfile\s+dir\s+(.*)$",
                            p_szSQL, re.IGNORECASE | re.DOTALL)
        if match_obj:
            self.c_SeedFileDir = str(match_obj.group(1)).strip()
            yield (
                None,
                None,
                None,
                None,
                "DATAHandler set successful!")
            return

        # 设置HDFS连接用户，如果不设置，将会默认为dr.who，即匿名用户
        match_obj = re.match(r"data\s+set\s+hdfsuser\s+(.*)$",
                            p_szSQL, re.IGNORECASE | re.DOTALL)
        if match_obj:
            self.c_HDFS_ConnectedUser = str(match_obj.group(1)).strip()
            yield (
                None,
                None,
                None,
                None,
                "DATAHandler set successful!")
            return

        # 创建数据文件, 根据末尾的rows来决定创建的行数
        # 此时，SQL语句中的回车换行符没有意义
        match_obj = re.match(r"data\s+create\s+(.*?)\s+file\s+(.*?)\((.*)\)(\s+)?rows\s+(\d+)(\s+)?$",
                            p_szSQL, re.IGNORECASE | re.DOTALL)
        if match_obj:
            m_filetype = str(match_obj.group(1)).strip()
            m_filename = str(match_obj.group(2)).strip().replace('\r', '').replace('\n', '')
            m_formula_str = str(match_obj.group(3).replace('\r', '').replace('\n', '').strip())
            m_rows = int(match_obj.group(5))
            self.Create_file(p_filetype=m_filetype,
                             p_filename=m_filename,
                             p_formula_str=m_formula_str,
                             p_rows=m_rows,
                             p_encoding=self.SQLOptions.get("RESULT_ENCODING"))
            yield (
                None,
                None,
                None,
                None,
                str(m_rows) + ' rows created Successful.')
            return

        match_obj = re.match(r"data\s+create\s+(.*?)\s+file\s+(.*?)\((.*)\)(\s+)?$",
                            p_szSQL, re.IGNORECASE | re.DOTALL)
        if match_obj:
            m_filetype = str(match_obj.group(1)).strip()
            m_filename = str(match_obj.group(2)).strip().replace('\r', '').replace('\n', '')
            m_formula_str = str(match_obj.group(3).strip())
            m_rows = 1
            self.Create_file(p_filetype=m_filetype,
                             p_filename=m_filename,
                             p_formula_str=m_formula_str,
                             p_rows=m_rows,
                             p_encoding=self.SQLOptions.get("RESULT_ENCODING"))
            yield (
                None,
                None,
                None,
                None,
                str(m_rows) + ' rows created Successful.')
            return

        #  在不同的文件中进行相互转换
        match_obj = re.match(r"data\s+create\s+(.*?)\s+file\s+(.*?)\s+from\s+(.*?)file(.*?)(\s+)?$",
                            p_szSQL, re.IGNORECASE | re.DOTALL)
        if match_obj:
            # 在不同的文件中相互转换
            self.Convert_file(p_srcfileType=str(match_obj.group(3)).strip(),
                              p_srcfilename=str(match_obj.group(4)).strip(),
                              p_dstfileType=str(match_obj.group(1)).strip(),
                              p_dstfilename=str(match_obj.group(2)).strip())
            yield (
                None,
                None,
                None,
                None,
                'file converted Successful.')
            return

        # 创建随机数Seed的缓存文件
        match_obj = re.match(r"data\s+create\s+(integer|string)\s+seeddatafile\s+(.*?)\s+"
                            r"length\s+(\d+)\s+rows\s+(\d+)\s+with\s+null\s+rows\s+(\d+)$",
                            p_szSQL, re.IGNORECASE | re.DOTALL)
        if match_obj:
            m_DataType = str(match_obj.group(1)).lstrip().rstrip()
            m_SeedFileName = str(match_obj.group(2)).lstrip().rstrip()
            m_DataLength = int(match_obj.group(3))
            m_nRows = int(match_obj.group(4))
            m_nNullValueCount = int(match_obj.group(5))
            self.create_seed_cache(data_type=m_DataType, data_length=m_DataLength, row_count=m_nRows,
                                      seed_name=m_SeedFileName, null_count=m_nNullValueCount)
            yield (
                None,
                None,
                None,
                None,
                'seed file created Successful.')
            return

        # 创建随机数Seed的缓存文件
        match_obj = re.match(r"data\s+create\s+(integer|string)\s+seeddatafile\s+(.*?)\s+"
                            r"length\s+(\d+)\s+rows\s+(\d+)(\s+)?$",
                            p_szSQL, re.IGNORECASE | re.DOTALL)
        if match_obj:
            m_DataType = str(match_obj.group(1)).lstrip().rstrip()
            m_SeedFileName = str(match_obj.group(2)).lstrip().rstrip()
            m_DataLength = int(match_obj.group(3))
            m_nRows = int(match_obj.group(4))
            self.create_seed_cache(data_type=m_DataType, data_length=m_DataLength,
                                      row_count=m_nRows, seed_name=m_SeedFileName)
            yield (
                None,
                None,
                None,
                None,
                'seed file created Successful.')
            return

        yield None, None, None, None, "Unknown data Command."
