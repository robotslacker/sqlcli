# -*- coding: utf-8 -*-
import os
import re


class DiffException(Exception):
    def __init__(self, message):
        Exception.__init__(self)
        self.message = message


class POSIXCompare:
    CompiledRegexPattern = {}

    def __init__(self):
        pass

    # 正则表达比较两个字符串
    # p_str1                  原字符串
    # p_str2                  正则表达式
    # p_compare_maskEnabled   是否按照正则表达式来判断是否相等
    # p_compare_ignorecase    是否忽略匹配中的大小写
    def compare_string(self, p_str1, p_str2,
                       p_compare_maskEnabled=False,
                       p_compare_ignorecase=False):
        # 如果两个字符串完全相等，直接返回
        if p_str1 == p_str2:
            return True

        # 如果忽略大小写的情况下，两个字符串的大写相同，则直接返回
        if p_compare_ignorecase:
            if p_str1.upper() == p_str2.upper():
                return True

        # 如果没有启用正则，则直接返回不相等
        if not p_compare_maskEnabled:
            return False

        # 用正则判断表达式是否相等
        try:
            if p_str2 in self.CompiledRegexPattern:
                m_CompiledPattern = self.CompiledRegexPattern[p_str2]
            else:
                m_CompiledPattern = re.compile(p_str2)
                self.CompiledRegexPattern[p_str2] = m_CompiledPattern
            if p_compare_ignorecase:
                matchObj = re.match(m_CompiledPattern, p_str1, re.IGNORECASE)
            else:
                matchObj = re.match(m_CompiledPattern, p_str1)
            if matchObj is None:
                return False
            elif str(matchObj.group()) != p_str1:
                return False
            else:
                return True
        except re.error:
            # 正则表达式错误，可能是由于这并非是一个正则表达式
            return False

    def compare(self,
                x,
                y,
                linenox,
                linenoy,
                p_compare_maskEnabled=False,
                p_compare_ignorecase=False):
        # LCS问题就是求两个字符串最长公共子串的问题。
        # 解法就是用一个矩阵来记录两个字符串中所有位置的两个字符之间的匹配情况，若是匹配则为1，否则为0。
        # 然后求出对角线最长的1序列，其对应的位置就是最长匹配子串的位置。

        # c           LCS数组
        # x           源数据
        # y           目的数据
        # linenox     源数据行数信息
        # linenoy     目的数据行数信息
        # i           源数据长度
        # j           目的数据长度
        # p_result    比对结果
        next_x = x
        next_y = y
        next_i = len(x) - 1
        next_j = len(y) - 1

        # This is our matrix comprised of list of lists.
        # We allocate extra row and column with zeroes for the base case of empty
        # sequence. Extra row and column is appended to the end and exploit
        # Python's ability of negative indices: x[-1] is the last elem.
        # 构建LCS数组
        c = [[0 for _ in range(len(y) + 1)] for _ in range(len(x) + 1)]
        for i, xi in enumerate(x):
            for j, yj in enumerate(y):
                if self.compare_string(xi, yj,
                                       p_compare_maskEnabled, p_compare_ignorecase):
                    c[i][j] = 1 + c[i - 1][j - 1]
                else:
                    c[i][j] = max(c[i][j - 1], c[i - 1][j])

        # 开始比较
        compare_result = True
        m_CompareDiffResult = []
        while True:
            if next_i < 0 and next_j < 0:
                break
            elif next_i < 0:
                compare_result = False
                m_CompareDiffResult.append("+{:>{}} ".format(linenoy[next_j], 6) + next_y[next_j])
                next_j = next_j - 1
            elif next_j < 0:
                compare_result = False
                m_CompareDiffResult.append("-{:>{}} ".format(linenox[next_i], 6) + next_x[next_i])
                next_i = next_i - 1
            elif self.compare_string(next_x[next_i], next_y[next_j],
                                     p_compare_maskEnabled, p_compare_ignorecase):
                m_CompareDiffResult.append(" {:>{}} ".format(linenox[next_i], 6) + next_x[next_i])
                next_i = next_i - 1
                next_j = next_j - 1
            elif c[next_i][next_j - 1] >= c[next_i - 1][next_j]:
                compare_result = False
                m_CompareDiffResult.append("+{:>{}} ".format(linenoy[next_j], 6) + next_y[next_j])
                next_j = next_j - 1
            elif c[next_i][next_j - 1] < c[next_i - 1][next_j]:
                compare_result = False
                m_CompareDiffResult.append("-{:>{}} ".format(linenox[next_i], 6) + next_x[next_i])
                next_i = next_i - 1
        return compare_result, m_CompareDiffResult

    def compare_text_files(self, file1, file2,
                           skiplines=None,
                           ignoreEmptyLine=False,
                           CompareWithMask=None,
                           CompareIgnoreCase=False,
                           CompareIgnoreTailOrHeadBlank=False,
                           CompareWorkEncoding='UTF-8',
                           CompareRefEncoding='UTF-8'):
        if not os.path.isfile(file1):
            raise DiffException('ERROR: %s is not a file' % file1)
        if not os.path.isfile(file2):
            raise DiffException('ERROR: %s is not a file' % file2)

        # 将比较文件加载到数组
        file1rawcontent = open(file1, mode='r', encoding=CompareWorkEncoding).readlines()

        file1content = open(file1, mode='r', encoding=CompareWorkEncoding).readlines()
        file2content = open(file2, mode='r', encoding=CompareRefEncoding).readlines()

        lineno1 = []
        lineno2 = []
        for m_nPos in range(0, len(file1content)):
            lineno1.append(m_nPos + 1)
        for m_nPos in range(0, len(file2content)):
            lineno2.append(m_nPos + 1)

        # 去掉filecontent中的回车换行
        for m_nPos in range(0, len(file1content)):
            if file1content[m_nPos].endswith('\n'):
                file1content[m_nPos] = file1content[m_nPos][:-1]
        for m_nPos in range(0, len(file2content)):
            if file2content[m_nPos].endswith('\n'):
                file2content[m_nPos] = file2content[m_nPos][:-1]

        # 去掉fileconent中的首尾空格
        if CompareIgnoreTailOrHeadBlank:
            for m_nPos in range(0, len(file1content)):
                file1content[m_nPos] = file1content[m_nPos].lstrip().rstrip()
            for m_nPos in range(0, len(file2content)):
                file2content[m_nPos] = file2content[m_nPos].lstrip().rstrip()

        # 去除在SkipLine里头的所有内容
        if skiplines is not None:
            m_nPos = 0
            while m_nPos < len(file1content):
                bMatch = False
                for pattern in skiplines:
                    if self.compare_string(file1content[m_nPos], pattern, p_compare_maskEnabled=True):
                        file1content.pop(m_nPos)
                        lineno1.pop(m_nPos)
                        bMatch = True
                        break
                if not bMatch:
                    m_nPos = m_nPos + 1

            m_nPos = 0
            while m_nPos < len(file2content):
                bMatch = False
                for pattern in skiplines:
                    if self.compare_string(file2content[m_nPos], pattern, p_compare_maskEnabled=True):
                        file2content.pop(m_nPos)
                        lineno2.pop(m_nPos)
                        bMatch = True
                        break
                if not bMatch:
                    m_nPos = m_nPos + 1

        # 去除所有的空行
        if ignoreEmptyLine:
            m_nPos = 0
            while m_nPos < len(file1content):
                if len(file1content[m_nPos].strip()) == 0:
                    file1content.pop(m_nPos)
                    lineno1.pop(m_nPos)
                else:
                    m_nPos = m_nPos + 1
            m_nPos = 0
            while m_nPos < len(file2content):
                if len(file2content[m_nPos].strip()) == 0:
                    file2content.pop(m_nPos)
                    lineno2.pop(m_nPos)
                else:
                    m_nPos = m_nPos + 1

        # 输出两个信息
        # 1：  Compare的结果是否存在dif，True/False
        # 2:   Compare的Dif列表，注意：这里是一个翻转的列表
        (m_CompareResult, m_CompareResultList) = self.compare(file1content, file2content, lineno1, lineno2,
                                                              p_compare_maskEnabled=CompareWithMask,
                                                              p_compare_ignorecase=CompareIgnoreCase)
        # 首先翻转数组
        # 随后从数组中补充进入被Skip掉的内容
        m_nLastPos = 0
        m_NewCompareResultList = []
        # 从列表中反向开始遍历， Step=-1
        for row in m_CompareResultList[::-1]:
            if row.startswith('+'):
                # 当前日志没有，Log中有的，忽略不计
                m_NewCompareResultList.append(row)
                continue
            elif row.startswith('-'):
                # 记录当前行号
                m_LineNo = int(row[1:7])
            elif row.startswith(' '):
                # 记录当前行号
                m_LineNo = int(row[0:7])
            else:
                raise DiffException("Missed line number. Bad compare result. [" + row + "]")
            if m_LineNo > (m_nLastPos + 1):
                for m_nPos in range(m_nLastPos + 1, m_LineNo):
                    m_NewCompareResultList.append("S{:>{}} ".format(m_nPos, 6) + file1rawcontent[m_nPos - 1])
                m_NewCompareResultList.append(row)
                m_nLastPos = m_LineNo
            else:
                m_NewCompareResultList.append(row)
                m_nLastPos = m_LineNo
        return m_CompareResult, m_NewCompareResultList


class CompareWrapper(object):
    def Process_SQLCommand(self, p_szSQL):
        m_szSQL = p_szSQL.strip()

        matchObj = re.match(r"kafka\s+connect\s+server\s+(.*)$",
                            m_szSQL, re.IGNORECASE | re.DOTALL)
        if matchObj:
            m_KafkaServer = str(matchObj.group(1)).strip()
            return None, None, None, None, "Kafka Server set successful."

        pass
