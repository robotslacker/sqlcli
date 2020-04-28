import re


def SQLAnalyze( p_SQLCommandPlainText):
    SQLCommands = p_SQLCommandPlainText.split('\n')
    SQLSplitResults = []
    SQLSplitResultsWithComments = []

    strNewSQL = None
    strOrigin_SQL = None
    b_inParaComments = False

    b_SQLCompleted = True

    for sql in SQLCommands:
        b_SQLCompleted = False

        # 备份原有SQL语句，可能包含注释信息
        if strOrigin_SQL is None:
            strOrigin_SQL = sql
        else:
            strOrigin_SQL = strOrigin_SQL + '\n' + sql

        # 如果SQL有行注释，去掉行注释信息
        if sql.find('--') != -1:
            sql = sql[0:sql.find('--')]

        # 如果SQL同一行内有段落注释，去掉段落注释
        m_nCommentsStart = sql.find('/*')
        m_nCommentsEnd = sql.find('*/')
        if (
                m_nCommentsStart != -1 and
                m_nCommentsEnd != -1 and
                m_nCommentsStart < m_nCommentsEnd
        ):
            sql = sql[0:m_nCommentsStart] + sql[m_nCommentsEnd + 2:]

        # 如果SQL有多段注释
        if (
                m_nCommentsStart != -1 and
                m_nCommentsEnd == -1
        ):
            sql = sql[0:m_nCommentsStart]
            b_inParaComments = True
        if (
                b_inParaComments and
                m_nCommentsEnd != -1
        ):
            sql = sql[m_nCommentsEnd + 2:]
            b_inParaComments = False

        # 去掉末尾的空格
        sql = sql.rstrip()

        if strNewSQL is None:
            # 如果本行没有内容，跳过
            if len(sql) == 0:
                continue

            # 对于Load,Connect,Start,Set开头的语句要求当行结束
            if re.match(r'^load\s|^connect\s|^start\s|^set\s', sql, re.IGNORECASE):
                SQLSplitResults.append(sql)
                SQLSplitResultsWithComments.append(strOrigin_SQL)
                strOrigin_SQL = None
                b_SQLCompleted = True
                continue

            # 如果没有任何语句历史的前提下，输入/，直接插入一个空语句
            if sql == '/':
                SQLSplitResultsWithComments.append(strOrigin_SQL)
                strOrigin_SQL = None
                SQLSplitResults.append('')
                b_SQLCompleted = True
                continue

            # 如果第一行语句中找不到关键字，直接返回SQL
            strRegexPattern = '^CREATE\\s|' \
                              '^SELECT\\s|' \
                              '^UPDATE\\s|' \
                              '^DELETE\\s|' \
                              '^INSERT\\s'
            if not re.match(strRegexPattern, sql, re.IGNORECASE):
                SQLSplitResultsWithComments.append(strOrigin_SQL)
                strOrigin_SQL = None
                SQLSplitResults.append(sql)
                b_SQLCompleted = True
                continue
            else:
                # 找到了关键字，可能存在多行SQL
                if sql.endswith(';'):
                    # 单行SQL
                    SQLSplitResultsWithComments.append(strOrigin_SQL)
                    strOrigin_SQL = None
                    SQLSplitResults.append(sql[:-1])
                    b_SQLCompleted = True
                    continue
                else:
                    # 多行SQL
                    strNewSQL = sql
                    continue

        else:
            # 任何时候遇到/作为唯一内容的行，本语句段落结束
            if sql == '/':
                SQLSplitResultsWithComments.append(strOrigin_SQL)
                strOrigin_SQL = None
                SQLSplitResults.append(strNewSQL)
                strNewSQL = None
                b_SQLCompleted = True
                continue

            # 如果本行以；结尾，且不存在CREATE|DROP|REPLACE FUNCTION|PROCEDURE开头的语句，结束
            if sql.endswith(';'):
                strRegexPattern = '(^CREATE\\s+|^DROP\\s+|^REPLACE\\s+)(FUNCTION|PROCEDURE)'
                if len(sql) != 1:
                    strTemp = strNewSQL + '\n' + sql
                else:   # 本行只有一个；
                    strTemp = strNewSQL
                if not re.match(strRegexPattern, strTemp, re.IGNORECASE):
                    SQLSplitResultsWithComments.append(strOrigin_SQL)
                    strOrigin_SQL = None
                    SQLSplitResults.append(strTemp[:-1])
                    b_SQLCompleted = True
                    strNewSQL = None
                    continue

            # 其他情况下，语句直接追加到新语句中
            strNewSQL = strNewSQL + '\n' + sql

    if strNewSQL is None:
        b_SQLCompleted = True

    return b_SQLCompleted, SQLSplitResults, SQLSplitResultsWithComments
