# -*- coding: utf-8 -*-
import re
import copy
import os
import shlex
from .sqlcliexception import SQLCliException


def APIFormatWithPrefix(p_szCommentSQLScript, p_szOutputPrefix=""):
    # 如果是完全空行的内容，则跳过
    if len(p_szCommentSQLScript) == 0:
        return None

    # 把所有的SQL换行, 第一行加入[API >]， 随后加入[   >]
    m_FormattedString = None
    m_CommentSQLLists = p_szCommentSQLScript.split('\n')
    if len(p_szCommentSQLScript) >= 1:
        # 如果原来的内容最后一个字符就是回车换行符，split函数会在后面补一个换行符，这里要去掉，否则前端显示就会多一个空格
        if p_szCommentSQLScript[-1] == "\n":
            del m_CommentSQLLists[-1]

    # 拼接字符串
    bSQLPrefix = 'API> '
    for pos in range(0, len(m_CommentSQLLists)):
        if pos == 0:
            m_FormattedString = p_szOutputPrefix + bSQLPrefix + m_CommentSQLLists[pos]
        else:
            m_FormattedString = \
                m_FormattedString + '\n' + p_szOutputPrefix + bSQLPrefix + m_CommentSQLLists[pos]
        if len(m_CommentSQLLists[pos].strip()) != 0:
            bSQLPrefix = '   > '
    return m_FormattedString


def APIAnalyze(apiCommandPlainText):
    """ 分析API语句，返回如下内容：
        MulitLineAPIHint                是否为完整API请求定义， True：完成， False：不完整，需要用户继续输入
        APISplitResults                 包含所有API信息的一个数组，每一个API作为一个元素
        APISplitResultsWithComments     包含注释信息的API语句信息，数组长度和APISplitResults相同
        APIHints                        API的其他各种标志信息，根据APISplitResultsWithComments中的注释内容解析获得
    """
    return True, [], [], {}
