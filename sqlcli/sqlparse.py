# -*- coding: utf-8 -*-
import re
import copy


def SQLFormatWithPrefix(p_szCommentSQLScript):
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
    for m_nPos in range(0, len(m_CommentSQLLists)):
        if m_nPos == 0:
            m_FormattedString = 'SQL> ' + m_CommentSQLLists[m_nPos]
        else:
            m_FormattedString = \
                m_FormattedString + '\n' + '   > ' + m_CommentSQLLists[m_nPos]
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
                        r'^(\s+)?exit(\s+)?$|^(\s+)?quit(\s+)?$',
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
            strRegexPattern = r'^(\s+)?CREATE(\s+)?|^(\s+)?SELECT(\s+)?|^(\s+)?UPDATE(\s+)?|' \
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
        if SQLSplitResultsWithComments[m_nPos][-1:] == '\n':
            SQLSplitResultsWithComments[m_nPos] = SQLSplitResultsWithComments[m_nPos][:-1]

    return not m_bInMultiLineSQL, SQLSplitResults, SQLSplitResultsWithComments
