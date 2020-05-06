# -*- coding: utf-8 -*-
import re
import copy


def SQLFormatWithPrefix(p_szCommentSQLScript):
    # 把所有的SQL换行, 第一行加入[SQL >]， 随后加入[   >]
    m_FormattedString = None
    m_CommentSQLLists = p_szCommentSQLScript.split('\n')
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
                    # 注释已经结束
                    SQLCommands[m_nPos] = re.sub(r'.', ' ', SQLCommands[m_nPos][0:m_nBlockCommentsEnd + 2]) + \
                                          SQLCommands[m_nPos][m_nBlockCommentsEnd + 2:]
                    m_bInCommentBlock = False
                    continue
                else:
                    # 本行完全还在注释中
                    SQLCommands[m_nPos] = re.sub(r'.', ' ', SQLCommands[m_nPos])
                    break

    # 开始分析SQL
    SQLSplitResults = []
    SQLSplitResultsWithComments = []
    m_bInBlockSQL = False
    m_bInMultiLineSQL = False
    m_NewSQL = None
    m_NewSQLWithComments = None
    m_NewSQLWithCommentsLastPos = 0
    for m_nPos in range(0, len(SQLCommands)):
        if not m_bInMultiLineSQL:  # 没有在多行语句中
            # 这是一个单行语句, 要求单行结束， 属于内部执行，需要去掉其中的注释信息
            if re.match(r'^(\s+)?load\s|^(\s+)?connect\s|^(\s+)?start\s|^(\s+)?set\s|'
                        r'^(\s+)?exit(\s+)?$|^(\s+)?quit(\s+)?$',
                        SQLCommands[m_nPos], re.IGNORECASE):
                m_SQL = SQLCommands[m_nPos].strip()
                SQLSplitResults.append(m_SQL)
                m_CommentSQL = SQLCommandsWithComments[m_nPos]
                if m_nPos == len(SQLCommands) - 1:
                    # 如果本行就是最后一行
                    SQLSplitResultsWithComments.append(m_CommentSQL)
                    m_NewSQLWithCommentsLastPos = 0
                else:
                    # 一直找到下一个有效语句的开始
                    for m_CommentPos in range(m_nPos + 1, len(SQLCommands)):
                        if len(SQLCommands[m_CommentPos].lstrip()) != 0:
                            # 这是一个非完全空行，有效内容从第一个字符开始
                            nLeadSpace = len(SQLCommands[m_CommentPos]) - len(SQLCommands[m_CommentPos].lstrip())
                            m_CommentSQL = m_CommentSQL + '\n' + SQLCommandsWithComments[m_CommentPos][0:nLeadSpace]
                            m_NewSQLWithCommentsLastPos = nLeadSpace
                            SQLSplitResultsWithComments.append(m_CommentSQL)
                            break
                        else:
                            # 完全注释行
                            m_CommentSQL = m_CommentSQL + '\n' + SQLCommandsWithComments[m_CommentPos]
                continue

            # 如果本行只有唯一的内容，就是段落终止符的话，本语句没有任何意义
            if re.match(r'^/$', SQLCommands[m_nPos]):
                SQLSplitResults.append("")
                m_CommentSQL = SQLCommandsWithComments[m_nPos]
                if m_nPos == len(SQLCommands) - 1:
                    # 如果本行就是最后一行
                    SQLSplitResultsWithComments.append(m_CommentSQL)
                    m_NewSQLWithCommentsLastPos = 0
                else:
                    # 一直找到下一个有效语句的开始
                    for m_CommentPos in range(m_nPos + 1, len(SQLCommands)):
                        if len(SQLCommands[m_CommentPos].lstrip()) != 0:
                            # 这是一个非完全空行，有效内容从第一个字符开始
                            nLeadSpace = len(SQLCommands[m_CommentPos]) - len(SQLCommands[m_CommentPos].lstrip())
                            m_CommentSQL = m_CommentSQL + '\n' + SQLCommandsWithComments[m_CommentPos][0:nLeadSpace]
                            m_NewSQLWithCommentsLastPos = nLeadSpace
                            SQLSplitResultsWithComments.append(m_CommentSQL)
                            break
                        else:
                            # 完全注释行
                            m_CommentSQL = m_CommentSQL + '\n' + SQLCommandsWithComments[m_CommentPos]
                continue

            # 如何本行没有任何内容，就是空行，直接结束，本语句没有任何意义
            if SQLCommands[m_nPos].strip() == "":
                SQLSplitResults.append("")
                SQLSplitResultsWithComments.append("")
                continue

            # 如果本行每行没有包含任何关键字信息，则直接返回
            strRegexPattern = r'^(\s+)?CREATE(\s+)?|^(\s+)?SELECT(\s+)?|^(\s+)?UPDATE(\s+)?|' \
                              r'^(\s+)?DELETE(\s+)?|^(\s+)?INSERT(\s+)?|^(\s+)?__INTERNAL__(\s+)?|' \
                              r'^(\s+)?DROP(\s+)?|^(\s+)?REPLACE(\s+)?'
            if not re.match(strRegexPattern, SQLCommands[m_nPos], re.IGNORECASE):
                SQLSplitResults.append(SQLCommands[m_nPos])
                m_CommentSQL = SQLCommandsWithComments[m_nPos]
                if m_nPos == len(SQLCommands) - 1:
                    # 如果本行就是最后一行
                    SQLSplitResultsWithComments.append(m_CommentSQL)
                else:
                    # 一直找到下一个有效语句的开始
                    for m_CommentPos in range(m_nPos + 1, len(SQLCommands)):
                        if len(SQLCommands[m_CommentPos].lstrip()) != 0:
                            # 这是一个非完全空行，有效内容从第一个字符开始
                            nLeadSpace = len(SQLCommands[m_CommentPos]) - len(SQLCommands[m_CommentPos].lstrip())
                            m_CommentSQL = m_CommentSQL + '\n' + SQLCommandsWithComments[m_CommentPos][0:nLeadSpace]
                            m_NewSQLWithCommentsLastPos = nLeadSpace
                            SQLSplitResultsWithComments.append(m_CommentSQL)
                            break
                        else:
                            # 完全注释行
                            m_CommentSQL = m_CommentSQL + '\n' + SQLCommandsWithComments[m_CommentPos]
                continue

            # 以下本行内一定包含了关键字，是多行SQL，或者多段SQL, 也可能是单行；结尾
            m_NewSQL = SQLCommands[m_nPos]
            m_NewSQLWithComments = SQLCommandsWithComments[m_nPos][m_NewSQLWithCommentsLastPos:]
            if re.match(r'(.*);(\s+)?$', SQLCommands[m_nPos]):  # 本行以；结尾
                strRegexPattern = r'^((\s+)?CREATE(\s+)|^(\s+)?REPLACE(\s+))(.*)?(FUNCTION|PROCEDURE)'
                if not re.match(strRegexPattern, SQLCommands[m_nPos], re.IGNORECASE):
                    # 这不是一个存储过程，本行可以结束了
                    SQLSplitResults.append(SQLCommands[m_nPos])
                    if m_nPos == len(SQLCommands) - 1:
                        # 如果本行就是最后一行
                        SQLSplitResultsWithComments.append(m_NewSQLWithComments)
                        m_NewSQLWithCommentsLastPos = 0
                        m_NewSQLWithComments = None
                    else:
                        # 一直找到下一个有效语句的开始
                        for m_CommentPos in range(m_nPos + 1, len(SQLCommands)):
                            if len(SQLCommands[m_CommentPos].lstrip()) != 0:
                                # 这是一个非完全空行，有效内容从第一个字符开始
                                nLeadSpace = len(SQLCommands[m_CommentPos]) - len(SQLCommands[m_CommentPos].lstrip())
                                m_NewSQLWithComments = \
                                    m_NewSQLWithComments + '\n' + \
                                    SQLCommandsWithComments[m_CommentPos][0:nLeadSpace]
                                m_NewSQLWithCommentsLastPos = nLeadSpace
                                SQLSplitResultsWithComments.append(m_NewSQLWithComments)
                                m_NewSQLWithComments = None
                                break
                            else:
                                # 完全注释行
                                m_NewSQLWithComments = m_NewSQLWithComments + '\n' + \
                                                       SQLCommandsWithComments[m_CommentPos]
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
                m_NewSQLWithComments = m_NewSQLWithComments + '\n' + SQLCommandsWithComments[m_nPos]
                if m_nPos == len(SQLCommands) - 1:
                    # 如果本行就是最后一行
                    SQLSplitResultsWithComments.append(m_NewSQLWithComments)
                    m_NewSQLWithCommentsLastPos = 0
                    m_NewSQLWithComments = None
                else:
                    # 一直找到下一个有效语句的开始
                    for m_CommentPos in range(m_nPos + 1, len(SQLCommands)):
                        if len(SQLCommands[m_CommentPos].lstrip()) != 0:
                            # 这是一个非完全空行，有效内容从第一个字符开始
                            nLeadSpace = len(SQLCommands[m_CommentPos]) - len(SQLCommands[m_CommentPos].lstrip())
                            m_NewSQLWithComments = m_NewSQLWithComments + '\n' + SQLCommandsWithComments[m_CommentPos][
                                                                                 0:nLeadSpace]
                            m_NewSQLWithCommentsLastPos = nLeadSpace
                            SQLSplitResultsWithComments.append(m_NewSQLWithComments)
                            m_NewSQLWithComments = None
                            break
                        else:
                            # 完全注释行
                            m_NewSQLWithComments = m_NewSQLWithComments + '\n' + SQLCommandsWithComments[m_CommentPos]
                m_bInBlockSQL = False
                m_bInMultiLineSQL = False
                continue

            if m_bInBlockSQL:
                # 不可能终止，因为/已经被之前判断了
                m_NewSQL = m_NewSQL + '\n' + SQLCommands[m_nPos]
                m_NewSQLWithComments = m_NewSQLWithComments + '\n' + SQLCommandsWithComments[m_nPos]
            else:
                m_NewSQL = m_NewSQL + '\n' + SQLCommands[m_nPos]
                m_NewSQLWithComments = \
                    m_NewSQLWithComments + '\n' + \
                    SQLCommandsWithComments[m_nPos][m_NewSQLWithCommentsLastPos:]
                # 工作在多行语句中，查找;结尾的内容
                if re.match(r'(.*);(\s+)?$', SQLCommands[m_nPos]):  # 本行以；结尾
                    strRegexPattern = r'(^(\s+)?CREATE|^(\s+)?DROP|^(\s+)?REPLACE)(.*)?\s+(FUNCTION|PROCEDURE)(.*)?'
                    if not re.match(strRegexPattern, m_NewSQL, re.IGNORECASE):
                        # 这不是一个存储过程，本行可以结束了
                        SQLSplitResults.append(m_NewSQL)
                        if m_nPos == len(SQLCommands) - 1:
                            # 如果本行就是最后一行
                            SQLSplitResultsWithComments.append(m_NewSQLWithComments)
                            m_NewSQLWithCommentsLastPos = 0
                            m_NewSQLWithComments = None
                        else:
                            # 一直找到下一个有效语句的开始
                            for m_CommentPos in range(m_nPos + 1, len(SQLCommands)):
                                if len(SQLCommands[m_CommentPos].lstrip()) != 0:
                                    # 这是一个非完全空行，有效内容从第一个字符开始
                                    nLeadSpace = len(SQLCommands[m_CommentPos]) - len(
                                        SQLCommands[m_CommentPos].lstrip())
                                    m_NewSQLWithComments = \
                                        m_NewSQLWithComments + '\n' + \
                                        SQLCommandsWithComments[m_CommentPos][0:nLeadSpace]
                                    m_NewSQLWithCommentsLastPos = nLeadSpace
                                    SQLSplitResultsWithComments.append(m_NewSQLWithComments)
                                    m_NewSQLWithComments = None
                                    break
                                else:
                                    # 完全注释行
                                    m_NewSQLWithComments = m_NewSQLWithComments + '\n' + \
                                                           SQLCommandsWithComments[m_CommentPos]
                        m_bInMultiLineSQL = False
                        continue
                    else:
                        # 才发现这是一个存储过程
                        m_bInBlockSQL = True
                        m_bInMultiLineSQL = True
                else:
                    # 多行语句还没有结束
                    continue

    # 去掉注释信息中的最后一个回车换行符
    for m_nPos in range(0, len(SQLSplitResultsWithComments)):
        if SQLSplitResultsWithComments[m_nPos][-1:] == '\n':
            SQLSplitResultsWithComments[m_nPos] = SQLSplitResultsWithComments[m_nPos][:-1]

    return not m_bInMultiLineSQL, SQLSplitResults, SQLSplitResultsWithComments
