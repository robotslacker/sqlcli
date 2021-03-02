# -*- coding: utf-8 -*-
import re
import copy
import os
import shlex
from .sqlinternal import random_ascii_letters_and_digits


class SQLMapping(object):
    # MappingList = { Mapping_Name : Mapping_Contents}
    # Mapping_Contents = [ filename_pattern, match_roles[] ]
    # match_roles = [ Key, Value]
    m_SQL_MappingList = {}

    Console = None       # 控制台信息，用来打印日志内容

    def Load_SQL_Mappings(self, p_szTestScriptFileName, p_szSQLMappings):
        # 如果不带任何参数，或者参数为空，则清空SQLMapping信息
        if p_szSQLMappings is None:
            self.m_SQL_MappingList = {}
            return

        m_SQL_Mappings = shlex.shlex(p_szSQLMappings)
        m_SQL_Mappings.whitespace = ','
        m_SQL_Mappings.quotes = "'"
        m_SQL_Mappings.whitespace_split = True

        # 如果没有传递脚本名称，则认为是在Console中执行
        if p_szTestScriptFileName is None:
            m_szTestScriptFileName = os.path.join(os.getcwd(), "dummy.txt")
        else:
            m_szTestScriptFileName = p_szTestScriptFileName

        # 循环处理每一个Mapping信息
        for m_SQL_MappingFile in list(m_SQL_Mappings):
            m_SQL_MappingBaseName = None
            m_SQL_MappingFullName = None

            if os.path.isfile(m_SQL_MappingFile):
                # 用户提供的是全路径名
                m_SQL_MappingBaseName = os.path.basename(m_SQL_MappingFile)  # 不包含路径的文件名
                m_SQL_MappingFullName = os.path.abspath(m_SQL_MappingFile)
            elif os.path.isfile(os.path.join(
                    os.path.dirname(m_szTestScriptFileName),
                    m_SQL_MappingFile)):
                # 用户提供的是当前目录下的文件
                m_SQL_MappingBaseName = os.path.basename(m_SQL_MappingFile)  # 不包含路径的文件名
                m_SQL_MappingFullName = os.path.join(
                    os.path.dirname(m_szTestScriptFileName), m_SQL_MappingFile)
            elif os.path.isfile(os.path.join(
                    os.path.dirname(m_szTestScriptFileName),
                    m_SQL_MappingFile + ".map")):
                # 用户提供的是当前目录下的文件
                m_SQL_MappingBaseName = os.path.basename(m_SQL_MappingFile)  # 不包含路径的文件名
                m_SQL_MappingFullName = os.path.join(
                    os.path.dirname(m_szTestScriptFileName),
                    m_SQL_MappingFile + ".map")
            if m_SQL_MappingFullName is None or m_SQL_MappingBaseName is None:
                # 压根没有找到这个文件
                if "SQLCLI_DEBUG" in os.environ:
                    print("SQLCLI-0003::  Mapping file [" + m_SQL_MappingFile + "] not found.")
                continue

            # 加载配置文件
            if "SQLCLI_DEBUG" in os.environ:
                print("Loading ... [" + m_SQL_MappingFullName + "]")
            with open(m_SQL_MappingFullName, 'r') as f:
                m_SQL_Mapping_Contents = f.readlines()

            # 去掉配置文件中的注释信息, 包含空行，单行完全注释，以及文件行内注释的注释部分
            m_nPos = 0
            while m_nPos < len(m_SQL_Mapping_Contents):
                if (m_SQL_Mapping_Contents[m_nPos].startswith('#') and
                    not m_SQL_Mapping_Contents[m_nPos].startswith('#.')) or \
                        len(m_SQL_Mapping_Contents[m_nPos]) == 0:
                    m_SQL_Mapping_Contents.pop(m_nPos)
                else:
                    m_nPos = m_nPos + 1
            for m_nPos in range(0, len(m_SQL_Mapping_Contents)):
                if m_SQL_Mapping_Contents[m_nPos].find('#') != -1 and \
                        not m_SQL_Mapping_Contents[m_nPos].startswith('#.'):
                    m_SQL_Mapping_Contents[m_nPos] = \
                        m_SQL_Mapping_Contents[m_nPos][0:m_SQL_Mapping_Contents[m_nPos].find('#')]

            # 分段加载配置文件
            m_inSection = False
            m_szNamePattern = None
            m_szMatchRules = []
            m_szFileMatchRules = []
            for m_szLine in m_SQL_Mapping_Contents:
                m_szLine = m_szLine.strip()
                if not m_inSection and m_szLine.startswith("#.") and m_szLine.endswith(':'):
                    # 文件注释开始
                    m_inSection = True
                    m_szNamePattern = m_szLine[2:-1]  # 去掉开始的的#.以前最后的:
                    m_szMatchRules = []
                    continue
                if m_inSection and m_szLine == "#.":
                    # 文件注释结束
                    m_szFileMatchRules.append([m_szNamePattern, m_szMatchRules])
                    m_szNamePattern = None
                    m_szMatchRules = []
                    m_inSection = False
                    continue
                if m_inSection:
                    # 文件配置段中的内容
                    if m_szLine.find('=>') != -1:
                        m_szMatchRule = [m_szLine[0:m_szLine.find('=>')].strip(),
                                         m_szLine[m_szLine.find('=>') + 2:].strip()]
                        m_szMatchRules.append(m_szMatchRule)
                    continue

            # 每个文件的配置都加载到MappingList中
            self.m_SQL_MappingList[m_SQL_MappingBaseName] = m_szFileMatchRules

    @staticmethod
    def ReplaceMacro_Env(p_arg):
        m_EnvName = p_arg[0].replace("'", "").replace('"', "").strip()
        if m_EnvName in os.environ:
            return os.environ[m_EnvName]
        else:
            return ""

    @staticmethod
    def ReplaceMacro_Random(p_arg):
        m_RandomType = p_arg[0].replace("'", "").replace('"', "").strip()
        if m_RandomType.lower() == "random_ascii_letters_and_digits":
            if str(p_arg[1]).isnumeric():
                m_RandomLength = int(p_arg[1])
                return random_ascii_letters_and_digits([m_RandomLength, ])
            else:
                return ""

    def ReplaceSQL(self, p_szSQL, p_Key, p_Value):
        # 首先查找是否有匹配的内容，如果没有，直接返回
        try:
            m_SearchResult = re.search(p_Key, p_szSQL, re.DOTALL)
        except re.error as ex:
            print("[WARNING] Invalid regex pattern. [" + str(p_Key) + "]  " + repr(ex), file=self.Console)
            return p_szSQL

        if m_SearchResult is None:
            return p_szSQL
        else:
            # 记录匹配到的内容
            m_SearchedKey = m_SearchResult.group()

        # 将内容用{}来进行分割，以处理各种内置的函数，如env等
        m_row_struct = re.split('[{}]', p_Value)
        if len(m_row_struct) == 1:
            # 没有任何内置的函数， 直接替换掉结果就可以了
            m_Value = p_Value
        else:
            # 存在内置的函数，即数据中存在{}包括的内容
            for m_nRowPos in range(0, len(m_row_struct)):
                if re.search(r'env(.*)', m_row_struct[m_nRowPos], re.IGNORECASE):
                    # 函数的参数处理，即函数的参数可能包含， 如 func(a,b)，将a，b作为数组记录
                    m_function_struct = re.split(r'[(,)]', m_row_struct[m_nRowPos])
                    # 特殊替换本地标识符:1， :1表示=>前面的内容
                    for m_nPos in range(1, len(m_function_struct)):
                        if m_function_struct[m_nPos] == ":1":
                            m_function_struct[m_nPos] = m_SearchedKey

                    # 执行替换函数
                    if len(m_function_struct) <= 1:
                        print("[WARNING] Invalid env macro, use env(). [" + str(p_Key) + "=>" + str(p_Value) + "]",
                              file=self.Console)
                        m_row_struct[m_nRowPos] = ""
                    else:
                        m_row_struct[m_nRowPos] = self.ReplaceMacro_Env(m_function_struct[1:])

                if re.search(r'random(.*)', m_row_struct[m_nRowPos], re.IGNORECASE):
                    # 函数的参数处理，即函数的参数可能包含， 如 func(a,b)，将a，b作为数组记录
                    m_function_struct = re.split(r'[(,)]', m_row_struct[m_nRowPos])
                    # 特殊替换本地标识符:1， :1表示=>前面的内容
                    for m_nPos in range(1, len(m_function_struct)):
                        if m_function_struct[m_nPos] == ":1":
                            m_function_struct[m_nPos] = m_SearchedKey
                    # 执行替换函数
                    if len(m_function_struct) <= 1:
                        print("[WARNING] Invalid random macro, use random(). "
                              "[" + str(p_Key) + "=>" + str(p_Value) + "]",
                              file=self.Console)
                        m_row_struct[m_nRowPos] = ""
                    else:
                        m_row_struct[m_nRowPos] = self.ReplaceMacro_Random(m_function_struct[1:])

            # 重新拼接字符串
            m_Value = None
            for m_nRowPos in range(0, len(m_row_struct)):
                if m_Value is None:
                    m_Value = m_row_struct[m_nRowPos]
                else:
                    m_Value = m_Value + str(m_row_struct[m_nRowPos])

        try:
            m_ResultSQL = re.sub(p_Key, m_Value, p_szSQL, flags=re.DOTALL)
        except re.error as ex:
            print("[WARNING] Invalid regex pattern in ReplaceSQL. " +
                  "[" + str(p_Key) + "]:[" + m_Value + "]:[" +
                  p_szSQL + "]  " +
                  repr(ex), file=self.Console)
            raise ex
        return m_ResultSQL

    def RewriteSQL(self, p_szTestScriptFileName, p_szSQL):
        # 检查是否存在sql mapping文件
        if len(self.m_SQL_MappingList) == 0:
            return p_szSQL

        # 获得绝对文件名
        if p_szTestScriptFileName is not None:
            m_TestScriptFileName = os.path.basename(p_szTestScriptFileName)
        else:
            # 用户从Console上启动，没有脚本文件名
            m_TestScriptFileName = "Console"

        # 检查文件名是否匹配
        # 如果一个字符串在多个匹配规则中出现，可能被多次匹配。后一次匹配的依据是前一次匹配的结果
        m_New_SQL = p_szSQL
        for m_MappingFiles in self.m_SQL_MappingList:                           # 所有的SQL Mapping信息
            m_MappingFile_Contents = self.m_SQL_MappingList[m_MappingFiles]     # 具体的一个SQL Mapping文件
            for m_Mapping_Contents in m_MappingFile_Contents:                   # 具体的一个映射信息
                try:
                    if re.match(m_Mapping_Contents[0], m_TestScriptFileName):       # 文件名匹配
                        for (m_Key, m_Value) in m_Mapping_Contents[1]:              # 内容遍历
                            try:
                                m_New_SQL = self.ReplaceSQL(m_New_SQL, m_Key, m_Value)
                            except re.error:
                                print("[WARNING] Invalid regex pattern in ReplaceSQL. ",
                                      file=self.Console)
                except re.error as ex:
                    print("[WARNING] Invalid regex pattern in filename match. " +
                          "[" + str(m_Mapping_Contents[0]) + "]:[" + m_TestScriptFileName + "]:[" +
                          m_MappingFiles + "]  " +
                          repr(ex), file=self.Console)
                    print("[WARNING] Your mapping config has been ignored.")
                    self.m_SQL_MappingList = {}
        return m_New_SQL


def SQLFormatWithPrefix(p_szCommentSQLScript, p_szOutputPrefix=""):
    # 把所有的SQL换行, 第一行加入[SQL >]， 随后加入[   >]
    m_FormattedString = None
    m_CommentSQLLists = p_szCommentSQLScript.split('\n')
    # 从最后一行开始，依次删除空行，空行内容不打印
    for m_nPos in range(len(m_CommentSQLLists), 0, -1):
        if len(m_CommentSQLLists[m_nPos-1].strip()) == 0:
            del m_CommentSQLLists[-1]
        else:
            break

    # 拼接字符串
    bSQLPrefix = 'SQL> '
    for m_nPos in range(0, len(m_CommentSQLLists)):
        if m_nPos == 0:
            m_FormattedString = p_szOutputPrefix + bSQLPrefix + m_CommentSQLLists[m_nPos]
        else:
            m_FormattedString = \
                m_FormattedString + '\n' + p_szOutputPrefix + bSQLPrefix + m_CommentSQLLists[m_nPos]
        if len(m_CommentSQLLists[m_nPos].strip()) != 0:
            bSQLPrefix = '   > '
    return m_FormattedString


def SQLAnalyze(p_SQLCommandPlainText):
    """ 分析SQL语句，返回如下内容：
        MulitLineSQLHint                该SQL是否为完整SQL， True：完成， False：不完整，需要用户继续输入
        SQLSplitResults                 包含所有SQL信息的一个数组，每一个SQL作为一个元素
        SQLSplitResultsWithComments     包含注释信息的SQL语句信息，数组长度和SQLSplitResults相同
        SQLHints                        SQL的其他各种标志信息，根据SQLSplitResultsWithComments中的注释内容解析获得
    """
    SQLCommands = p_SQLCommandPlainText.split('\n')

    # 首先备份原始的SQL
    SQLCommandsWithComments = copy.copy(SQLCommands)

    # 从SQLCommands中删除所有的注释信息，但是不能删除ECHO中的注释信息
    m_bInCommentBlock = False         # 是否在多行注释内
    m_bInEchoSQL = False              # 是否在ECHO语句内部
    for m_nPos in range(0, len(SQLCommands)):
        while True:  # 不排除一行内多个注释的问题
            # 首先处理特殊的ECHO信息
            if m_bInEchoSQL:
                # ECHO信息已经结束
                if re.match(r'echo\s+off', SQLCommands[m_nPos], re.IGNORECASE):
                    m_bInEchoSQL = False
                break
            # 如果当前是ECHO文件开头，进入ECHO模式
            if re.match(r'echo\s+.*', SQLCommands[m_nPos], re.IGNORECASE):
                m_bInEchoSQL = True
                break

            # 如果存在行内的块注释, 且当前不是在注释中
            if not m_bInCommentBlock:
                # 如果存在行注释
                m_nLineCommentStart = SQLCommands[m_nPos].find('--')
                if m_nLineCommentStart != -1:
                    SQLCommands[m_nPos] = SQLCommands[m_nPos][0:m_nLineCommentStart] + \
                                          re.sub(r'.', ' ', SQLCommands[m_nPos][m_nLineCommentStart:])
                    continue

                # 检查段落注释
                m_nBlockCommentsStart = SQLCommands[m_nPos].find('/*')
                m_nBlockCommentsEnd = SQLCommands[m_nPos].find('*/')

                # 单行内块注释
                if (
                        m_nBlockCommentsStart != -1 and
                        m_nBlockCommentsEnd != -1 and
                        m_nBlockCommentsStart < m_nBlockCommentsEnd
                ):
                    SQLCommands[m_nPos] = SQLCommands[m_nPos][0:m_nBlockCommentsStart] + \
                                          re.sub(r'.', ' ',
                                                 SQLCommands[m_nPos][m_nBlockCommentsStart:m_nBlockCommentsEnd + 2]) + \
                                          SQLCommands[m_nPos][m_nBlockCommentsEnd + 2:]
                    continue

                # 单行内块注释开始
                if (
                        m_nBlockCommentsStart != -1 and
                        m_nBlockCommentsEnd == -1
                ):
                    m_bInCommentBlock = True
                    SQLCommands[m_nPos] = \
                        SQLCommands[m_nPos][0:m_nBlockCommentsStart] + \
                        re.sub(r'.', ' ', SQLCommands[m_nPos][m_nBlockCommentsStart:])
                    break

                # 没有找到任何注释
                break
            else:
                # 当前已经在注释中，需要找到*/ 来标记注释的结束
                m_nBlockCommentsEnd = SQLCommands[m_nPos].find('*/')
                if m_nBlockCommentsEnd != -1:
                    # 注释已经结束， 注释部分替换为空格
                    SQLCommands[m_nPos] = re.sub(r'.', ' ', SQLCommands[m_nPos][0:m_nBlockCommentsEnd + 2]) + \
                                          SQLCommands[m_nPos][m_nBlockCommentsEnd + 2:]
                    m_bInCommentBlock = False
                    continue
                else:
                    # 本行完全还在注释中，全部替换为空格
                    SQLCommands[m_nPos] = re.sub(r'.', ' ', SQLCommands[m_nPos])
                    break

    # 开始分析SQL

    # SQL分析的结果， 这两个（包含注释，不包含注释）的数组长度相等，若只有注释，则SQL结果中为空白
    SQLSplitResults = []
    # 包含注释的SQL语句分析结果
    SQLSplitResultsWithComments = []

    # 是否在代码段内部
    m_bInBlockSQL = False
    # 是否在多行语句内部
    m_bInMultiLineSQL = False
    # 是否在ECHO语句内部
    m_bInEchoSQL = False

    # 拼接好的SQL
    m_NewSQL = None
    # 拼接好的SQL，包含了注释信息
    m_NewSQLWithComments = None
    # 当前的Echo信息
    m_EchoMessages = None

    # 下一段注释将从m_NewSQLWithCommentPos的m_NewSQLWithCommentsLastPos字符开始
    m_NewSQLWithCommentsLastPos = 0            # 注释已经截止到的行中列位置
    m_NewSQLWithCommentPos = 0                 # 注释已经截止到的行号

    for m_nPos in range(0, len(SQLCommands)):
        # 首先处理特殊的ECHO信息
        if m_bInEchoSQL:
            # ECHO信息已经结束
            if re.match(r'echo\s+off', SQLCommands[m_nPos], re.IGNORECASE):
                # 添加ECHO信息到解析后的SQL中
                if m_EchoMessages is not None:
                    SQLSplitResults.append(m_EchoMessages)
                    SQLSplitResultsWithComments.append(m_EchoMessages)
                SQLSplitResults.append(SQLCommands[m_nPos])
                SQLSplitResultsWithComments.append(SQLCommands[m_nPos])
                m_EchoMessages = None
                m_bInEchoSQL = False
                continue
            # 当前是一段ECHO信息
            if m_EchoMessages is None:
                m_EchoMessages = SQLCommands[m_nPos]
                continue
            else:
                m_EchoMessages = m_EchoMessages + "\n" + SQLCommands[m_nPos]
                continue
        # 如果当前是ECHO文件开头，则当前的ECHO语句作为一个SQL返回，随后进入ECHO模式
        if re.match(r'echo\s+.*', SQLCommands[m_nPos], re.IGNORECASE):
            SQLSplitResults.append(SQLCommands[m_nPos])
            SQLSplitResultsWithComments.append(SQLCommands[m_nPos])
            m_bInEchoSQL = True
            continue

        # 正常处理其他SQL
        if not m_bInMultiLineSQL:  # 没有在多行语句中
            # 这是一个单行语句, 要求单行结束， 属于内部执行，需要去掉其中的注释信息
            if re.match(r'^(\s+)?loaddriver\s|^(\s+)?connect\s|^(\s+)?start\s|^(\s+)?set\s|'
                        r'^(\s+)?exit(\s+)?$|^(\s+)?quit(\s+)?$|^(\s+)?loadsqlmap\s',
                        SQLCommands[m_nPos], re.IGNORECASE):
                m_SQL = SQLCommands[m_nPos].strip()
                SQLSplitResults.append(m_SQL)
                SQLSplitResultsWithComments.append(SQLCommandsWithComments[m_nPos])
                if m_nPos == len(SQLCommands) - 1:
                    # 如果本行就是最后一行
                    m_NewSQLWithCommentsLastPos = 0
                else:
                    # 从当前语句开始，一直找到下一个有效语句的开始，中间全部是注释
                    m_CommentSQL = None
                    for m_CommentPos in range(m_nPos + 1, len(SQLCommands)):
                        if len(SQLCommands[m_CommentPos].lstrip()) != 0:
                            # 这是一个非完全空行，有效内容从第一个字符开始
                            nLeadSpace = len(SQLCommands[m_CommentPos]) - len(SQLCommands[m_CommentPos].lstrip())
                            if m_CommentSQL is None:
                                m_CommentSQL = SQLCommandsWithComments[m_CommentPos][0:nLeadSpace]
                            else:
                                m_CommentSQL = m_CommentSQL + '\n' + SQLCommandsWithComments[m_CommentPos][0:nLeadSpace]
                            m_NewSQLWithCommentsLastPos = nLeadSpace
                            m_NewSQLWithCommentPos = m_CommentPos
                            break
                        else:
                            # 完全注释行
                            if m_CommentSQL is None:
                                m_CommentSQL = SQLCommandsWithComments[m_CommentPos]
                            else:
                                m_CommentSQL = m_CommentSQL + '\n' + SQLCommandsWithComments[m_CommentPos]
                            m_NewSQLWithCommentPos = m_CommentPos + 1
                    SQLSplitResults.append("")
                    SQLSplitResultsWithComments.append(m_CommentSQL)
                continue

            # 如果本行只有唯一的内容，就是段落终止符的话，本语句没有意义，不会执行；但是注释有意义
            # / 不能送给SQL解析器
            if re.match(r'^(\s+)?/(\s+)?$', SQLCommands[m_nPos]):
                SQLSplitResults.append("")
                SQLSplitResultsWithComments.append(SQLCommandsWithComments[m_nPos])
                if m_nPos == len(SQLCommands) - 1:
                    # 如果本行就是最后一行
                    m_NewSQLWithCommentsLastPos = 0
                else:
                    # 从当前语句开始，一直找到下一个有效语句的开始，中间全部是注释
                    m_CommentSQL = None
                    for m_CommentPos in range(m_nPos + 1, len(SQLCommands)):
                        if len(SQLCommands[m_CommentPos].lstrip()) != 0:
                            # 这是一个非完全空行，有效内容从第一个字符开始
                            nLeadSpace = len(SQLCommands[m_CommentPos]) - len(SQLCommands[m_CommentPos].lstrip())
                            if m_CommentSQL is None:
                                m_CommentSQL = SQLCommandsWithComments[m_CommentPos][0:nLeadSpace]
                            else:
                                m_CommentSQL = m_CommentSQL + '\n' + SQLCommandsWithComments[m_CommentPos][0:nLeadSpace]
                            m_NewSQLWithCommentsLastPos = nLeadSpace
                            m_NewSQLWithCommentPos = m_CommentPos
                            break
                        else:
                            # 完全注释行
                            if m_CommentSQL is None:
                                m_CommentSQL = SQLCommandsWithComments[m_CommentPos]
                            else:
                                m_CommentSQL = m_CommentSQL + '\n' + SQLCommandsWithComments[m_CommentPos]
                            m_NewSQLWithCommentPos = m_CommentPos + 1
                    SQLSplitResults.append("")
                    SQLSplitResultsWithComments.append(m_CommentSQL)
                continue

            # 如何本行没有任何内容，就是空行，直接结束，本语句没有任何意义
            if SQLCommands[m_nPos].strip() == "":
                if m_NewSQLWithCommentPos > m_nPos:
                    continue

                # 从当前语句开始，一直找到下一个有效语句的开始，中间全部是注释
                m_CommentSQL = SQLCommandsWithComments[m_nPos]
                for m_CommentPos in range(m_nPos + 1, len(SQLCommands)):
                    if len(SQLCommands[m_CommentPos].lstrip()) != 0:
                        # 这是一个非完全空行，有效内容从第一个字符开始
                        nLeadSpace = len(SQLCommands[m_CommentPos]) - len(SQLCommands[m_CommentPos].lstrip())
                        if m_CommentSQL is None:
                            m_CommentSQL = SQLCommandsWithComments[m_CommentPos][0:nLeadSpace]
                        else:
                            m_CommentSQL = m_CommentSQL + '\n' + SQLCommandsWithComments[m_CommentPos][0:nLeadSpace]
                        m_NewSQLWithCommentsLastPos = nLeadSpace
                        m_NewSQLWithCommentPos = m_CommentPos
                        break
                    else:
                        # 完全注释行
                        if m_CommentSQL is None:
                            m_CommentSQL = SQLCommandsWithComments[m_CommentPos]
                        else:
                            m_CommentSQL = m_CommentSQL + '\n' + SQLCommandsWithComments[m_CommentPos]
                        m_NewSQLWithCommentPos = m_CommentPos + 1
                        m_NewSQLWithCommentsLastPos = 0
                SQLSplitResults.append("")
                SQLSplitResultsWithComments.append(m_CommentSQL)
                continue

            # 如果本行每行没有包含任何关键字信息，则直接返回
            strRegexPattern = r'^(\s+)?CREATE(\s+)?|^(\s+)?(\()?SELECT(\s+)?|^(\s+)?UPDATE(\s+)?|' \
                              r'^(\s+)?DELETE(\s+)?|^(\s+)?INSERT(\s+)?|^(\s+)?__INTERNAL__(\s+)?|' \
                              r'^(\s+)?DROP(\s+)?|^(\s+)?REPLACE(\s+)?|^(\s+)?LOAD(\s+)?|' \
                              r'^(\s+)?MERGE(\s+)?|^(\s+)?DECLARE(\s+)?|^(\s+)?BEGIN(\s+)?|' \
                              r'^(\s+)?ALTER(\s+)?|^(\s+)?WITH(\s+)?'
            if not re.match(strRegexPattern, SQLCommands[m_nPos], re.IGNORECASE):
                SQLSplitResults.append(SQLCommands[m_nPos])
                SQLSplitResultsWithComments.append(SQLCommandsWithComments[m_nPos])
                if m_nPos == len(SQLCommands) - 1:
                    # 如果本行就是最后一行
                    m_NewSQLWithCommentsLastPos = 0
                else:
                    # 从当前语句开始，一直找到下一个有效语句的开始，中间全部是注释
                    m_CommentSQL = None
                    for m_CommentPos in range(m_nPos + 1, len(SQLCommands)):
                        if len(SQLCommands[m_CommentPos].lstrip()) != 0:
                            # 这是一个非完全空行，有效内容从第一个字符开始
                            nLeadSpace = len(SQLCommands[m_CommentPos]) - len(SQLCommands[m_CommentPos].lstrip())
                            if m_CommentSQL is None:
                                m_CommentSQL = SQLCommandsWithComments[m_CommentPos][0:nLeadSpace]
                            else:
                                m_CommentSQL = m_CommentSQL + '\n' + SQLCommandsWithComments[m_CommentPos][0:nLeadSpace]
                            m_NewSQLWithCommentsLastPos = nLeadSpace
                            m_NewSQLWithCommentPos = m_CommentPos
                            break
                        else:
                            # 完全注释行
                            if m_CommentSQL is None:
                                m_CommentSQL = SQLCommandsWithComments[m_CommentPos]
                            else:
                                m_CommentSQL = m_CommentSQL + '\n' + SQLCommandsWithComments[m_CommentPos]
                            m_NewSQLWithCommentPos = m_CommentPos + 1
                    SQLSplitResults.append("")
                    SQLSplitResultsWithComments.append(m_CommentSQL)
                continue

            # 以下本行内一定包含了关键字，是多行SQL，或者多段SQL, 也可能是单行；结尾
            m_NewSQL = SQLCommands[m_nPos]
            m_NewSQLWithComments = SQLCommandsWithComments[m_nPos][m_NewSQLWithCommentsLastPos:]
            if re.match(r'(.*);(\s+)?$', SQLCommands[m_nPos]):  # 本行以；结尾
                strRegexPattern = \
                    r'^((\s+)?CREATE(\s+)|^(\s+)?REPLACE(\s+))((\s+)?(OR)?(\s+)?(REPLACE)?(\s+)?)?(FUNCTION|PROCEDURE)'
                strRegexPattern2 = \
                    r'^(\s+)?DECLARE(\s+)|^(\s+)?BEGIN(\s+)'
                if not re.match(strRegexPattern, SQLCommands[m_nPos], re.IGNORECASE) and \
                        not re.match(strRegexPattern2, SQLCommands[m_nPos], re.IGNORECASE):
                    # 这不是一个存储过程，本行可以结束了
                    SQLSplitResults.append(SQLCommands[m_nPos])
                    SQLSplitResultsWithComments.append(SQLCommandsWithComments[m_nPos])
                    if m_nPos == len(SQLCommands) - 1:
                        # 如果本行就是最后一行
                        m_NewSQLWithCommentsLastPos = 0
                    else:
                        # 从当前语句开始，一直找到下一个有效语句的开始，中间全部是注释
                        m_CommentSQL = None
                        for m_CommentPos in range(m_nPos + 1, len(SQLCommands)):
                            if len(SQLCommands[m_CommentPos].lstrip()) != 0:
                                # 这是一个非完全空行，有效内容从第一个字符开始
                                nLeadSpace = len(SQLCommands[m_CommentPos]) - len(SQLCommands[m_CommentPos].lstrip())
                                if m_CommentSQL is None:
                                    m_CommentSQL = SQLCommandsWithComments[m_CommentPos][0:nLeadSpace]
                                else:
                                    m_CommentSQL = m_CommentSQL + '\n' + SQLCommandsWithComments[m_CommentPos][
                                                                         0:nLeadSpace]
                                m_NewSQLWithCommentsLastPos = nLeadSpace
                                m_NewSQLWithCommentPos = m_CommentPos
                                break
                            else:
                                # 完全注释行
                                if m_CommentSQL is None:
                                    m_CommentSQL = SQLCommandsWithComments[m_CommentPos]
                                else:
                                    m_CommentSQL = m_CommentSQL + '\n' + SQLCommandsWithComments[m_CommentPos]
                                m_NewSQLWithCommentPos = m_CommentPos + 1
                        SQLSplitResults.append("")
                        SQLSplitResultsWithComments.append(m_CommentSQL)
                    continue
                else:
                    # 这是一个存储过程
                    m_bInBlockSQL = True
                    m_bInMultiLineSQL = True
            else:
                # 不知道是啥，至少是一个语句段， NewSQL,CommentSQL已经被记录，跳到下一行处理
                m_bInMultiLineSQL = True
        else:  # 工作在多行语句中
            # 本行只有唯一的内容，就是段落终止符的话，语句应该可以提交了
            if re.match(r'^(\s+)?/(\s+)?$', SQLCommands[m_nPos]):
                SQLSplitResults.append(m_NewSQL)
                # 注释信息包含/符号
                SQLSplitResultsWithComments.append(m_NewSQLWithComments + "\n/")
                if m_nPos == len(SQLCommands) - 1:
                    # 如果本行就是最后一行
                    m_NewSQLWithCommentsLastPos = 0
                else:
                    # 从当前语句开始，一直找到下一个有效语句的开始，中间全部是注释
                    m_CommentSQL = None
                    for m_CommentPos in range(m_nPos + 1, len(SQLCommands)):
                        if len(SQLCommands[m_CommentPos].lstrip()) != 0:
                            # 这是一个非完全空行，有效内容从第一个字符开始
                            nLeadSpace = len(SQLCommands[m_CommentPos]) - len(SQLCommands[m_CommentPos].lstrip())
                            if m_CommentSQL is None:
                                m_CommentSQL = SQLCommandsWithComments[m_CommentPos][0:nLeadSpace]
                            else:
                                m_CommentSQL = m_CommentSQL + '\n' + SQLCommandsWithComments[m_CommentPos][0:nLeadSpace]
                            m_NewSQLWithCommentsLastPos = nLeadSpace
                            m_NewSQLWithCommentPos = m_CommentPos
                            break
                        else:
                            # 完全注释行
                            if m_CommentSQL is None:
                                m_CommentSQL = SQLCommandsWithComments[m_CommentPos]
                            else:
                                m_CommentSQL = m_CommentSQL + '\n' + SQLCommandsWithComments[m_CommentPos]
                            m_NewSQLWithCommentPos = m_CommentPos + 1
                    SQLSplitResults.append("")
                    SQLSplitResultsWithComments.append(m_CommentSQL)
                m_bInBlockSQL = False
                m_bInMultiLineSQL = False
                continue

            if m_bInBlockSQL:
                # 只要开始不是/，就一直拼接下去
                m_NewSQL = m_NewSQL + '\n' + SQLCommands[m_nPos]
                m_NewSQLWithComments = m_NewSQLWithComments + '\n' + SQLCommandsWithComments[m_nPos]
            else:
                # 多行语句，首先进行SQL的拼接
                m_NewSQL = m_NewSQL + '\n' + SQLCommands[m_nPos]
                m_NewSQLWithComments = \
                    m_NewSQLWithComments + '\n' + \
                    SQLCommandsWithComments[m_nPos][m_NewSQLWithCommentsLastPos:]
                # 工作在多行语句中，查找;结尾的内容
                if re.match(r'(.*);(\s+)?$', SQLCommands[m_nPos]):  # 本行以；结尾
                    # 查找这个多行语句是否就是一个存储过程
                    strRegexPattern = \
                        r'^((\s+)?CREATE(\s+)|' \
                        r'^(\s+)?REPLACE(\s+))((\s+)?(OR)?(\s+)?(REPLACE)?(\s+)?)?(FUNCTION|PROCEDURE)'
                    strRegexPattern2 = \
                        r'^(\s+)?DECLARE(\s+)|^(\s+)?BEGIN(\s+)'
                    if not re.match(strRegexPattern, m_NewSQL, re.IGNORECASE) and \
                            not re.match(strRegexPattern2, m_NewSQL, re.IGNORECASE):
                        # 这不是一个存储过程，遇到了；本行可以结束了
                        SQLSplitResults.append(m_NewSQL)
                        SQLSplitResultsWithComments.append(m_NewSQLWithComments)
                        # 从当前语句开始，一直找到下一个有效语句的开始，中间全部是注释
                        m_CommentSQL = None
                        for m_CommentPos in range(m_nPos + 1, len(SQLCommands)):
                            if len(SQLCommands[m_CommentPos].lstrip()) != 0:
                                # 这是一个非完全空行，有效内容从第一个字符开始
                                nLeadSpace = len(SQLCommands[m_CommentPos]) - len(SQLCommands[m_CommentPos].lstrip())
                                if m_CommentSQL is None:
                                    m_CommentSQL = SQLCommandsWithComments[m_CommentPos][0:nLeadSpace]
                                else:
                                    m_CommentSQL = m_CommentSQL + '\n' + SQLCommandsWithComments[m_CommentPos][
                                                                         0:nLeadSpace]
                                m_NewSQLWithCommentsLastPos = nLeadSpace
                                m_NewSQLWithCommentPos = m_CommentPos
                                break
                            else:
                                # 完全注释行
                                if m_CommentSQL is None:
                                    m_CommentSQL = SQLCommandsWithComments[m_CommentPos]
                                else:
                                    m_CommentSQL = m_CommentSQL + '\n' + SQLCommandsWithComments[m_CommentPos]
                                m_NewSQLWithCommentPos = m_CommentPos + 1
                        SQLSplitResults.append("")
                        SQLSplitResultsWithComments.append(m_CommentSQL)
                        m_bInMultiLineSQL = False
                        continue
                    else:
                        # 才发现这是一个存储过程
                        # 之前语句已经拼接过，所以这里标记后，直接开始下一个循环
                        m_bInBlockSQL = True
                        m_bInMultiLineSQL = True
                        continue
                else:
                    # 多行语句还没有结束
                    # 之前语句已经拼接过，所以这里标记后，直接开始下一个循环
                    continue

    # 如果当前语句没有结束，但是文件结束了，送最后一段到SQL引擎中，忽略其实际内容是什么，也许不能执行
    if m_bInMultiLineSQL:
        SQLSplitResults.append(m_NewSQL)
        SQLSplitResultsWithComments.append(m_NewSQLWithComments)

    # 去掉SQL语句中的最后一个； ORACLE数据库不支持带有；的语句
    for m_nPos in range(0, len(SQLSplitResults)):
        # 去掉行尾的空格
        SQLSplitResults[m_nPos] = SQLSplitResults[m_nPos].rstrip()
        # 去掉行尾的最后一个分号, “但是如果是END；结尾的，最后的；不能去掉”
        if SQLSplitResults[m_nPos][-1:] == ';' and not SQLSplitResults[m_nPos].upper().endswith("END;"):
            SQLSplitResults[m_nPos] = SQLSplitResults[m_nPos][:-1]

    # 去掉注释信息中的最后一个回车换行符
    for m_nPos in range(0, len(SQLSplitResultsWithComments)):
        if SQLSplitResultsWithComments[m_nPos] is not None:
            if SQLSplitResultsWithComments[m_nPos][-1:] == '\n':
                SQLSplitResultsWithComments[m_nPos] = SQLSplitResultsWithComments[m_nPos][:-1]
        else:
            SQLSplitResultsWithComments[m_nPos] = ""

    # 解析SQLHints
    m_SQLHints = []
    m_SQLHint = {}
    for m_nPos in range(0, len(SQLSplitResultsWithComments)):
        if len(SQLSplitResults[m_nPos]) == 0:
            # 这里为一个注释信息，解析注释信息中是否包含必要的tag
            for line in SQLSplitResultsWithComments[m_nPos].splitlines():
                # [Hint]  Scenario:XXXX   -- 相关SQL的Scenariox信息，仅仅作为日志信息供查看
                matchObj = re.search(r"^(\s+)?--(\s+)?\[Hint](\s+)?Scenario:(.*)", line,
                                     re.IGNORECASE | re.DOTALL)
                if matchObj:
                    m_SQLHint["SCENARIO"] = matchObj.group(4)

                matchObj = re.search(r"^(\s+)?--(\s+)?\[(\s+)?Scenario:(.*)]", line,
                                     re.IGNORECASE | re.DOTALL)
                if matchObj:
                    m_SQLHint["SCENARIO"] = matchObj.group(4)

                # [Hint]  order           -- SQLCli将会把随后的SQL语句进行排序输出，原程序的输出顺序被忽略
                matchObj = re.search(r"^(\s+)?--(\s+)?\[Hint](\s+)?order", line,
                                     re.IGNORECASE | re.DOTALL)
                if matchObj:
                    m_SQLHint["Order"] = True

                # [Hint]  LogFilter      -- SQLCli会过滤随后显示的输出信息，对于符合过滤条件的，将会被过滤
                matchObj = re.search(r"^(\s+)?--(\s+)?\[Hint](\s+)?LogFilter(\s+)(.*)", line,
                                     re.IGNORECASE | re.DOTALL)
                if matchObj:
                    m_SQLHint["LogFilter"] = matchObj.group(5)

                # [Hint]  LogMask      -- SQLCli会掩码随后显示的输出信息，对于符合掩码条件的，将会被掩码
                matchObj = re.search(r"^(\s+)?--(\s+)?\[Hint](\s+)?LogMask(\s+)(.*)=>(.*)", line,
                                     re.IGNORECASE | re.DOTALL)
                if matchObj:
                    m_SQLHint["LogMask"] = [matchObj.group(5), matchObj.group(6)]

            m_SQLHints.append({})
        else:
            # 这里是一个可执行SQL信息
            # 将这个SQL之前所有解析注释信息送到SQL的标志中，并同时清空当前的SQL标志信息
            m_SQLHints.append(m_SQLHint)
            m_SQLHint = {}

    return not m_bInMultiLineSQL, SQLSplitResults, SQLSplitResultsWithComments, m_SQLHints
