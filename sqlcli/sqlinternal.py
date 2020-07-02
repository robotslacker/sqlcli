# -*- coding: utf-8 -*-
import fs
import re
import os
from kafka import KafkaProducer
from .sqlcliexception import SQLCliException
import datetime
import random
from hdfs.client import Client
from hdfs.util import HdfsError
import traceback
from glob import glob


# 缓存seed文件，来加速后面的随机函数random_from_seed工作
seed_cache = {}
# 全局内存文件系统句柄
g_MemoryFSHandler = None


# 创建seed文件，默认在SQLCLI_HOME/data下
def Create_SeedCacheFile(
        p_szDataType,                      # 种子文件的数据类型，目前支持String和integer
        p_nDataLength,                     # 每个随机数的最大数据长度
        p_nRows,                           # 种子文件的行数，即随机数的数量
        p_szSeedName,
        p_nNullValueCount=0):
    if "SQLCLI_HOME" not in os.environ:
        raise SQLCliException("Missed SQLCLI_HOME, please set it first. Seed file will created in SQLCLI_HOME/data")

    # 如果不存在数据目录，创建它
    datadir = os.path.join(os.environ["SQLCLI_HOME"], "data")
    if not os.path.exists(datadir):
        os.makedirs(datadir)

    # 检查输入的参数
    if p_szDataType.upper() not in ('STRING', 'INTEGER'):
        raise SQLCliException("Data Type must be String or integer.")
    if p_nDataLength <= 0:
        raise SQLCliException("Data Length must great then zero.")
    if p_nRows <= 0:
        raise SQLCliException("rows must great then zero.")

    if p_szDataType.upper() == "STRING":
        buf = []
        for n in range(0, p_nRows):
            buf.append(random_ascii_letters_and_digits([p_nDataLength, ]) + "\n")
        if p_nNullValueCount > 0:
            for n in range(0, p_nNullValueCount):
                m_nPos = random.randint(0, p_nRows - 1)
                buf[m_nPos] = "\n"
        seed_file = os.path.join(os.environ["SQLCLI_HOME"], "data", p_szSeedName + ".seed")
        with open(seed_file, 'w') as f:
            f.writelines(buf)

    if p_szDataType.upper() == "INTEGER":
        buf = []
        for n in range(0, p_nRows):
            buf.append(random_digits([p_nDataLength, ]) + "\n")
        if p_nNullValueCount > 0:
            for n in range(0, p_nNullValueCount):
                m_nPos = random.randint(0, p_nRows - 1)
                buf[m_nPos] = "\n"
        seed_file = os.path.join(os.environ["SQLCLI_HOME"], "data", p_szSeedName + ".seed")
        with open(seed_file, 'w') as f:
            f.writelines(buf)


# 缓存seed文件，默认在SQLCLI_HOME/data下
def Load_SeedCacheFile():
    if "SQLCLI_HOME" not in os.environ:
        raise SQLCliException("Missed SQLCLI_HOME, please set it first. Seed file will created in $SQLCLI_HOME/data")

    m_seedpath = os.path.join(os.environ["SQLCLI_HOME"], "data", "*.seed")
    for seed_file in glob(m_seedpath):
        m_seedName = os.path.basename(seed_file)[:-5]            # 去掉.seed的后缀
        with open(seed_file, 'r') as f:
            m_seedlines = f.readlines()
        for m_nPos in range(0, len(m_seedlines)):
            if m_seedlines[m_nPos].endswith("\n"):
                m_seedlines[m_nPos] = m_seedlines[m_nPos][:-1]   # 去掉回车换行
        seed_cache[m_seedName] = m_seedlines


# 返回随机的Boolean类型
def random_boolean(p_arg):
    if p_arg:
        pass              # 这里用来避免p_arg参数无用的编译器警告
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


# 第一个参数是seed的名字
# 第二个参数是截取的最大长度
def random_from_seed(p_arg):
    m_SeedName = str(p_arg[0])
    m_nMaxLength = int(p_arg[1])

    # 如果还没有加载种子，就先加载
    n = len(seed_cache)
    if n == 0:
        Load_SeedCacheFile()

    # 在种子中查找需要的内容
    if m_SeedName in seed_cache:
        n = len(seed_cache[m_SeedName])
        m_RandomRow = seed_cache[m_SeedName][random.randint(0, n - 1)]
        if len(m_RandomRow) < m_nMaxLength:
            m_RandomResult = m_RandomRow
        else:
            m_RandomResult = m_RandomRow[0:m_nMaxLength]
        return m_RandomResult
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
    m_row_struct = re.split('[{}]', p_formula_str)
    m_return_row_struct = []

    for m_nRowPos in range(0, len(m_row_struct)):
        if re.search('random_ascii_lowercase|random_ascii_uppercase|random_ascii_letters' +
                     '|random_digits|identity|random_ascii_letters_and_digits|random_from_seed' +
                     '|random_date|random_timestamp|random_boolean',
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
            if m_function_struct[0].upper() == "RANDOM_DATE":
                m_call_out_struct.append(random_date)
                m_call_out_struct.append(m_function_struct[1:])
            if m_function_struct[0].upper() == "RANDOM_TIMESTAMP":
                m_call_out_struct.append(random_timestamp)
                m_call_out_struct.append(m_function_struct[1:])
            if m_function_struct[0].upper() == "RANDOM_BOOLEAN":
                m_call_out_struct.append(random_boolean)
                m_call_out_struct.append(m_function_struct[1:])
            m_return_row_struct.append(m_call_out_struct)
        else:
            m_return_row_struct.append(m_row_struct[m_nRowPos])
    return m_return_row_struct


def get_final_string(p_row_struct):
    m_Result = ""
    for col in p_row_struct:
        if isinstance(col, list):
            m_Result = m_Result + col[0](col[1])
        else:
            m_Result = m_Result + col
    return m_Result


def Create_file(p_filetype, p_filename, p_formula_str, p_rows, p_options):
    try:
        m_producer = None
        m_topicname = None
        m_output = None
        m_HDFS_Handler = None
        m_filename = None

        if p_filetype.upper() == "MEM":
            m_fs = fs.open_fs('mem://')
            m_filename = p_filename
            m_output = m_fs.open(m_filename, 'w')
        elif p_filetype.upper() == "FS":
            m_fs = fs.open_fs('./')
            m_filename = p_filename
            m_output = m_fs.open(m_filename, 'w')
        elif p_filetype.upper() == "KAFKA":
            if p_options["KAFKA_SERVERS"] is None or len(p_options["KAFKA_SERVERS"].strip()) == 0:
                raise SQLCliException("Please set KAFKA_SERVERS first")
            m_producer = KafkaProducer(bootstrap_servers=p_options["KAFKA_SERVERS"])
            m_topicname = p_filename
        elif p_filetype.upper() == "HDFS":
            # HDFS 文件格式： http://node:port/xx/yy/cc.dat
            # 注意这里的node和port都是webfs端口，不是rpc端口
            m_Protocal = p_filename.split("://")[0]
            m_NodePort = p_filename[len(m_Protocal)+3:].split("/")[0]
            m_WebFSURL = m_Protocal + "://" + m_NodePort
            m_WebFSDir, m_filename = os.path.split(p_filename[len(m_WebFSURL):])
            m_HDFS_Handler = Client(m_WebFSURL,
                                    m_WebFSDir,
                                    proxy=None, session=None)
        else:
            raise SQLCliException("Unknown file format.")

        m_row_struct = parse_formula_str(p_formula_str)
        buf = []
        if p_filetype.upper() == "HDFS":              # 处理HDFS文件写入
            # 总是覆盖服务器上的文件
            # 每10W行提交一次服务器文件, 以避免内存的OOM问题
            if p_rows < 100000:
                with m_HDFS_Handler.write(hdfs_path=m_filename, overwrite=True) as m_output:
                    for i in range(0, p_rows):
                        m_output.write((get_final_string(m_row_struct) + "\n").encode())
            else:
                for i in range(0, p_rows // 100000):
                    if i == 0:
                        with m_HDFS_Handler.write(hdfs_path=m_filename, overwrite=True) as m_output:
                            for j in range(0, 100000):
                                m_output.write((get_final_string(m_row_struct) + "\n").encode())
                    else:
                        with m_HDFS_Handler.write(hdfs_path=m_filename, append=True) as m_output:
                            for j in range(0, 100000):
                                m_output.write((get_final_string(m_row_struct) + "\n").encode())
                with m_HDFS_Handler.write(hdfs_path=m_filename, append=True) as m_output:
                    for i in range(0, p_rows % 100000):
                        m_output.write((get_final_string(m_row_struct) + "\n").encode())
        elif p_filetype.upper() == "KAFKA":           # 处理Kafka数据写入
            for i in range(0, p_rows):
                m_producer.send(m_topicname, get_final_string(m_row_struct).encode())
            m_producer.flush()
        else:                                          # 处理普通文件写入
            for i in range(0, p_rows):
                buf.append(get_final_string(m_row_struct) + '\n')
                if len(buf) == 100000:  # 为了提高IO效率，每10W条写入文件一次
                    m_output.writelines(buf)
                    buf = []
            if len(buf) != 0:
                m_output.writelines(buf)
            m_output.close()
    except SQLCliException as e:
        raise SQLCliException(e.message)
    except Exception as e:
        if "SQLCLI_DEBUG" in os.environ:
            print('traceback.print_exc():\n%s' % traceback.print_exc())
            print('traceback.format_exc():\n%s' % traceback.format_exc())
        raise SQLCliException(repr(e))
    return


def Convert_file(p_srcfileType, p_srcfilename, p_dstfileType, p_dstfilename):
    try:
        global g_MemoryFSHandler
        if p_srcfileType.upper() == "MEM":
            m_srcFSType = "MEM"
            if g_MemoryFSHandler is None:
                g_MemoryFSHandler = fs.open_fs('mem://')
                m_srcFS = g_MemoryFSHandler
            else:
                m_srcFS = g_MemoryFSHandler
            m_srcFileName = p_srcfilename
        elif p_srcfileType.upper() == "FS":
            m_srcFSType = "FS"
            m_srcFS = fs.open_fs('./')
            m_srcFileName = p_srcfilename
        elif p_srcfileType.upper() == "HDFS":
            m_srcFSType = "HDFS"
            m_srcFullFileName = p_srcfilename
            m_Protocal = m_srcFullFileName.split("://")[0]
            m_NodePort = m_srcFullFileName[len(m_Protocal)+3:].split("/")[0]
            m_WebFSURL = m_Protocal + "://" + m_NodePort
            m_WebFSDir, m_srcFileName = os.path.split(m_srcFullFileName[len(m_WebFSURL):])
            m_srcFS = Client(m_WebFSURL, m_WebFSDir, proxy=None, session=None)
        else:
            m_srcFS = None
            m_srcFileName = None
            m_srcFSType = "Not Supported"

        if p_dstfileType.upper() == "MEM":
            m_dstFSType = "MEM"
            if g_MemoryFSHandler is None:
                g_MemoryFSHandler = fs.open_fs('mem://')
                m_dstFS = g_MemoryFSHandler
            else:
                m_dstFS = g_MemoryFSHandler
            m_dstFileName = p_dstfilename
        elif p_dstfileType.upper() == "FS":
            m_dstFSType = "FS"
            m_dstFS = fs.open_fs('./')
            m_dstFileName = p_dstfilename
        elif p_dstfileType.upper() == "HDFS":
            m_dstFSType = "HDFS"
            m_dstFullFileName = p_dstfilename
            m_Protocal = m_dstFullFileName.split("://")[0]
            m_NodePort = m_dstFullFileName[len(m_Protocal)+3:].split("/")[0]
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
                    m_Contents = m_reader.read(8192*10240)
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
                    m_Contents = m_reader.read(8192*10240)
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
