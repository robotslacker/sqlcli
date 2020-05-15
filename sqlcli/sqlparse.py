# -*- coding: utf-8 -*-
import re
import copy
import os
import shlex


class SQLMapping(object):
    # MappingList = { Mapping_Name : Mapping_Contents}
    # Mapping_Contents = [ filename_pattern, match_roles[] ]
    # match_roles = [ Key, Value]
    m_SQL_MappingList = {}

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
                    m_SQL_MappingFile + ".map")):
                # 用户提供的是当前目录下的文件
                 m_SQL_MappingBaseName = os.path.basename(m_SQL_MappingFile)  # 不包含路径的文件名
                 m_SQL_MappingFullName = os.path.join(
                    os.path.dirname(m_szTestScriptFileName),
                    m_SQL_MappingFile + ".map")
            else:
                # 从系统目录中查找文件
                if 'SQLCLI_HOME' in os.environ:
                    if os.path.exists(os.path.join(
                            os.path.join(os.environ['SQLCLI_HOME'], 'mapping'),
                            m_SQL_MappingFile + ".map"
                    )):
                        m_SQL_MappingBaseName = os.path.basename(m_SQL_MappingFile)  # 不包含路径的文件名
                        m_SQL_MappingFullName = os.path.join(
                            os.path.join(os.environ['SQLCLI_HOME'], 'mapping'),
                            m_SQL_MappingFile + ".map"
                        )
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

    def ReplaceSQL(self, p_szSQL, p_Key, p_Value):
        # 首先查找是否有匹配的内容，如果没有，直接返回
        m_SearchResult = re.search(p_Key, p_szSQL, re.DOTALL)
        if m_SearchResult is None:
            return p_szSQL
        else:
            # 记录匹配到的内容
            m_SearchedKey = m_SearchResult.group()

        m_row_struct = re.split('[{}]', p_Value)
        m_Value = ""
        if len(m_row_struct) == 1:
            m_Value = p_Value
        else:
            for m_nRowPos in range(0, len(m_row_struct)):
                if re.search(r'env(.*)', m_row_struct[m_nRowPos], re.IGNORECASE):
                    m_function_struct = re.split(r'[(,)]', m_row_struct[m_nRowPos])
                    # 替换本地标识符:1
                    for m_nPos in range(1, len(m_function_struct)):
                        if m_function_struct[m_nPos] == ":1":
                            m_function_struct[m_nPos] = m_SearchedKey
                    # 执行替换函数
                    if m_function_struct[0].upper() == "ENV":
                        m_Value = self.ReplaceMacro_Env(m_function_struct[1:])
        m_szSQL = re.sub(p_Key, m_Value, p_szSQL, flags=re.DOTALL)
        return m_szSQL

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
        m_New_SQL = p_szSQL
        for m_MappingFiles in self.m_SQL_MappingList:                           # 所有的SQL Mapping信息
            m_MappingFile_Contents = self.m_SQL_MappingList[m_MappingFiles]     # 具体的一个SQL Mapping文件
            for m_Mapping_Contents in m_MappingFile_Contents:                   # 具体的一个映射信息
                if re.match(m_Mapping_Contents[0], m_TestScriptFileName):       # 文件名匹配
                    for (m_Key, m_Value) in m_Mapping_Contents[1]:              # 内容遍历
                        m_New_SQL = self.ReplaceSQL(p_szSQL, m_Key, m_Value)
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
    SQLCommands = p_SQLCommandPlainText.split('\n')

    # 首先备份原始的SQL
    SQLCommandsWithComments = copy.copy(SQLCommands)

    # 从SQLCommands中删除所有的注释信息
    m_bInCommentBlock = False
    for m_nPos in range(0, len(SQLCommands)):
        while True:  # 不排除一行内多个注释的问题
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

    # 拼接好的SQL
    m_NewSQL = None
    # 拼接好的SQL，包含了注释信息
    m_NewSQLWithComments = None

    # 下一段注释将从m_NewSQLWithCommentPos的m_NewSQLWithCommentsLastPos字符开始
    m_NewSQLWithCommentsLastPos = 0            # 注释已经截止到的行中列位置
    m_NewSQLWithCommentPos = 0                 # 注释已经截止到的行号
    for m_nPos in range(0, len(SQLCommands)):
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
            if re.match(r'^/$', SQLCommands[m_nPos]):
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
                              r'^(\s+)?MERGE(\s+)?'
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
                strRegexPattern = r'^((\s+)?CREATE(\s+)|^(\s+)?REPLACE(\s+))(.*)?(FUNCTION|PROCEDURE)'
                if not re.match(strRegexPattern, SQLCommands[m_nPos], re.IGNORECASE):
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
            if re.match(r'^/$', SQLCommands[m_nPos]):
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
                    strRegexPattern = r'(^(\s+)?CREATE|^(\s+)?DROP|^(\s+)?REPLACE)(.*)?\s+(FUNCTION|PROCEDURE)(.*)?'
                    if not re.match(strRegexPattern, m_NewSQL, re.IGNORECASE):
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
        if SQLSplitResults[m_nPos][-1:] == ';':
            SQLSplitResults[m_nPos] = SQLSplitResults[m_nPos][:-1]

    # 去掉注释信息中的最后一个回车换行符
    for m_nPos in range(0, len(SQLSplitResultsWithComments)):
        if SQLSplitResultsWithComments[m_nPos] is not None:
            if SQLSplitResultsWithComments[m_nPos][-1:] == '\n':
                SQLSplitResultsWithComments[m_nPos] = SQLSplitResultsWithComments[m_nPos][:-1]
        else:
            SQLSplitResultsWithComments[m_nPos] = ""

    return not m_bInMultiLineSQL, SQLSplitResults, SQLSplitResultsWithComments
